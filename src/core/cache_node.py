import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import psutil
import pyarrow as pa

from core.arrow_memory_management import ArrowMemoryManager
from core.bloom_filter import BloomFilter
from core.cache_data_model import CacheEntry, CacheKey, CachePolicy, PartitionInfo
from core.eviction_policy import (
    CustomEvictionPolicy,
    LFUEvictionPolicy,
    LRUEvictionPolicy,
)
from core.lru_cache import LRUCache
from iceberg_management.metadata import IcebergMetadataManager
from storage.dataloader import S3DataLoader

logger = logging.getLogger(__name__)


class ArrowCacheNode:
    """Single Cache Node implementation."""

    def __init__(
        self,
        config: Dict[str, Any],
        max_memory_bytes: Optional[int] = None,
        cache_policy: CachePolicy = CachePolicy.LRU,
        enable_bloom_filters: bool = True,
    ) -> None:
        if max_memory_bytes is None:
            total_memory = psutil.virtual_memory().total
            max_memory_bytes = int(total_memory * 0.8)

        self.config = config

        # --- single, unified cache store ---
        # Previously there were two separate stores (self.cache + self.cache_entries)
        # that were never synchronised.  Now every path reads/writes through
        # self.cache only.
        self.cache = LRUCache(max_size_bytes=config.get("max_cache_size", 2 * 1024 * 1024 * 1024))

        self.metadata_manager = IcebergMetadataManager(config["iceberg_catalog"])
        self.data_loader = S3DataLoader(config["aws"])
        self.memory_manager = ArrowMemoryManager(max_memory_bytes)

        # Partition metadata cache (table_location -> list[PartitionInfo])
        self.partition_info_cache: Dict[str, List[PartitionInfo]] = {}

        # Eviction policy
        self.eviction_policies = {
            CachePolicy.LRU: LRUEvictionPolicy(),
            CachePolicy.LFU: LFUEvictionPolicy(),
            CachePolicy.CUSTOM: CustomEvictionPolicy(),
        }
        self.current_policy = self.eviction_policies[cache_policy]

        # Index structures
        self.bloom_filters: Dict[str, BloomFilter] = {}
        self.min_max_stats: Dict[str, Dict[str, Tuple[Any, Any]]] = {}
        self.enable_bloom_filters = enable_bloom_filters

        # Threading
        self._lock = threading.RLock()

        # Start background maintenance (TTL cleanup, stats refresh)
        self._maintenance_thread = self._start_maintenance_thread()

    # ------------------------------------------------------------------
    # Background maintenance
    # ------------------------------------------------------------------

    def _start_maintenance_thread(self) -> threading.Thread:
        """Start and return a daemon background maintenance thread."""

        def maintenance_loop() -> None:
            while True:
                time.sleep(60)  # Run every minute
                try:
                    self._perform_maintenance()
                except Exception as e:
                    logger.error(f"Error in maintenance thread: {e}")

        thread = threading.Thread(
            target=maintenance_loop,
            daemon=True,
            name="cache-maintenance",
        )
        thread.start()
        return thread

    def _perform_maintenance(self) -> None:
        """Evict entries that have not been accessed within the TTL window."""
        with self._lock:
            current_time = time.time()
            # Snapshot entries outside the cache lock to avoid holding two locks.
            snapshot = self.cache.entries()
            expired_keys = [
                key
                for key, entry in snapshot.items()
                if (current_time - entry.last_accessed) > 3600  # 1-hour TTL
            ]
            for key in expired_keys:
                self._evict_entry(key)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_entry(self, key: str) -> None:
        """Evict a single cache entry and update the memory manager."""
        freed = self.cache.delete(key)
        if freed is not None:
            self.memory_manager.deallocate(freed)
            # Also clean up index structures
            self.bloom_filters.pop(key, None)
            self.min_max_stats.pop(key, None)
            logger.info(f"Evicted cache entry: {key}")

    def _create_cache_key(
        self, table_id: str, partition_spec: Dict, columns: Set[str]
    ) -> CacheKey:
        """Create a cache key from parameters."""
        partition_str = json.dumps(partition_spec, sort_keys=True) if partition_spec else "{}"
        return CacheKey(table_id, partition_str, columns)

    def _ensure_memory_available(self, required_bytes: int) -> None:
        """Ensure sufficient memory is available, evicting entries if necessary."""
        if self.memory_manager.allocate(required_bytes):
            return

        # Run eviction policy against a snapshot of current entries
        keys_to_evict = self.current_policy.should_evict(
            self.cache.entries(), required_bytes
        )

        for key in keys_to_evict:
            self._evict_entry(key)
            if self.memory_manager.allocate(required_bytes):
                return

        raise MemoryError(
            f"Cannot allocate {required_bytes} bytes even after eviction"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_table_partition(
        self,
        table_location: str,
        partition_values: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
    ) -> pa.Table:
        """Load a table partition into the unified cache and return it."""
        cache_key = self._create_cache_key(
            table_location, partition_values or {}, set(columns) if columns else set()
        )
        cache_key_str = str(cache_key)

        with self._lock:
            # --- cache hit ---
            entry = self.cache.get_entry(cache_key_str)
            if entry is not None:
                entry.touch()
                return entry.table

            # --- populate partition metadata if needed ---
            if table_location not in self.partition_info_cache:
                self.partition_info_cache[table_location] = (
                    self.metadata_manager.get_partition_info(table_location)
                )

            partitions = self.partition_info_cache[table_location]
            if not partitions:
                raise ValueError(f"No partitions found for table '{table_location}'")

            # --- find the best matching partition (fix: was always [0]) ---
            if partition_values:
                matching = [
                    p
                    for p in partitions
                    if all(
                        p.partition_spec.get(k) == v
                        for k, v in partition_values.items()
                    )
                ]
                partition_info = matching[0] if matching else partitions[0]
            else:
                partition_info = partitions[0]

            # --- load from object store ---
            table = self.data_loader.load_parquet_file(partition_info.file_path, columns)
            table_size = table.nbytes

            # Ensure memory is available (may evict other entries)
            self._ensure_memory_available(table_size)

            # Store in the unified cache
            self.cache.put(cache_key_str, table)

            # Build auxiliary indices
            self._build_indices(cache_key_str, table)

            return table

    def get_table_data(
        self,
        table_id: str,
        partition_filter: Optional[Dict] = None,
        columns: Optional[List[str]] = None,
    ) -> pa.Table:
        """Get table data, serving from the unified cache when possible."""
        cols_set = set(columns) if columns else set()
        cache_key = self._create_cache_key(table_id, partition_filter or {}, cols_set)
        cache_key_str = str(cache_key)

        # Cache hit (LRU-aware path)
        cached_table = self.cache.get(cache_key_str)
        if cached_table is not None:
            return cached_table

        # Cache miss — load from Iceberg/S3
        try:
            files = self.metadata_manager.get_data_files(table_id, partition_filter)
            if not files:
                return pa.table({})

            table = self.data_loader.load_multiple_parquet_files(
                [file.file_path for file in files], columns
            )

            self.cache.put(cache_key_str, table)
            return table

        except Exception as e:
            logger.error(f"Failed to load table data: {e}")
            raise

    def execute_query(self, sql: str) -> pa.Table:
        """Execute simple SQL queries (MVP implementation).

        TODO: replace with the DataFusion-backed QueryEngine.
        """
        sql_stripped = sql.strip().upper()

        if not sql_stripped.startswith("SELECT"):
            raise ValueError("Only SELECT queries supported in MVP")

        parts = sql_stripped.split()
        if "FROM" not in parts:
            raise ValueError("FROM clause required")

        from_idx = parts.index("FROM")
        table_id = parts[from_idx + 1].lower()

        return self.get_table_data(table_id)

    def get_all_tables(self) -> Dict[str, pa.Table]:
        """Return all currently cached tables.

        When the same logical table is cached across multiple partition keys the
        partitions are concatenated into a single Arrow Table so that the
        DataFusion QueryEngine can register one view per table name.
        """
        partitions_by_table: Dict[str, List[pa.Table]] = {}
        for key, entry in self.cache.entries().items():
            # Cache key format: "<table_id>#<partition_spec>#<cols_hash>"
            table_id = key.split("#")[0]
            partitions_by_table.setdefault(table_id, []).append(entry.table)

        return {
            table_id: pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            for table_id, tables in partitions_by_table.items()
        }

    def _build_indices(self, key: str, batch: pa.Table) -> None:
        """Build Bloom filter and min/max index structures for a cached entry."""
        if self.enable_bloom_filters:
            bloom_filter = BloomFilter()
            for col_name in batch.column_names:
                column = batch[col_name]
                if pa.types.is_string(column.type):
                    for val in column.to_pylist():
                        if val is not None:
                            bloom_filter.add(str(val))
            self.bloom_filters[key] = bloom_filter

        stats: Dict[str, Tuple[Any, Any]] = {}
        for col_name in batch.column_names:
            column = batch[col_name]
            if (
                pa.types.is_integer(column.type)
                or pa.types.is_floating(column.type)
                or pa.types.is_decimal(column.type)
                or pa.types.is_date(column.type)
                or pa.types.is_timestamp(column.type)
            ):
                try:
                    import pyarrow.compute as pc
                    stats[col_name] = (pc.min(column).as_py(), pc.max(column).as_py())
                except Exception:
                    pass
        self.min_max_stats[key] = stats

    def get_cache_stats(self) -> Dict[str, Any]:
        """Return cache utilisation statistics."""
        return self.cache.get_stats()

    def invalidate_table(self, table_id: str) -> None:
        """Invalidate all cached entries for a table.

        Uses cache.invalidate_prefix so that internal cache state (size
        accounting, OrderedDict ordering) stays consistent without reaching
        into private attributes.
        """
        # Cache keys start with "<table_id>#"
        count = self.cache.invalidate_prefix(f"{table_id}#")
        # Also clean up index structures for this table's keys
        keys_to_drop = [k for k in list(self.bloom_filters) if k.startswith(f"{table_id}#")]
        for k in keys_to_drop:
            self.bloom_filters.pop(k, None)
            self.min_max_stats.pop(k, None)
        logger.info(f"Invalidated {count} cache entries for table '{table_id}'")
