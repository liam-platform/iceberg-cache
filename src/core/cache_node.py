import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import psutil
import pyarrow as pa
import pyarrow.compute as pc

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

    def __init__(self, 
                 config: Dict[str, Any],
                 max_memory_bytes: Optional[int] = None,
                 cache_policy: CachePolicy = CachePolicy.LRU,
                 enable_bloom_filters: bool = True
    ) -> None:
        if max_memory_bytes is None:
            total_memory = psutil.virtual_memory().total
            max_memory_bytes = int(total_memory * 0.8)
        
        self.config = config
        self.cache = LRUCache(max_size_bytes=config.get(
            'max_cache_size', 2*1024*1024*1024))
        self.metadata_manager = IcebergMetadataManager(config['iceberg_catalog'])
        self.data_loader = S3DataLoader(config['aws'])
        self.memory_manager = ArrowMemoryManager(max_memory_bytes)

        # Cache storage
        self.cache_entries: Dict[str, CacheEntry] = {}
        self.partition_info_cache: Dict[str, List[PartitionInfo]] = {}
        
        # Eviction policy
        self.eviction_policies = {
            CachePolicy.LRU: LRUEvictionPolicy(),
            CachePolicy.LFU: LFUEvictionPolicy(),
            CachePolicy.CUSTOM: CustomEvictionPolicy()
        }
        self.current_policy = self.eviction_policies[cache_policy]
        
        # Index structures
        self.bloom_filters: Dict[str, BloomFilter] = {}
        self.min_max_stats: Dict[str, Dict[str, Tuple[Any, Any]]] = {}
        self.enable_bloom_filters = enable_bloom_filters
        
        # Threading
        self._lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
    
    def _start_maintenance_thread(self):
        """Start background maintenance thread"""
        def maintenance_loop():
            while True:
                time.sleep(60) # Run every minute
                try:
                    self._perform_maintenance()
                except Exception as e:
                    logger.error(f"Error in maintenance thread: {e}")

    def _perform_maintenance(self):
        """Perform background maintenance tasks"""
        with self._lock:
            # Clean up expired entries, update statistics, etc.
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self.cache_entries.items():
                # Remove entries older than 1 hour if not accessed recently
                if (current_time - entry.last_accessed) > 3600:
                    expired_keys.append(key)
            
            for key in expired_keys:
                self._evict_entry(key)
    
    def _evict_entry(self, key: str):
        """Evict a single cache key"""
        if key in self.cache_entries:
            entry = self.cache_entries[key]
            self.memory_manager.deallocate(entry.size_bytes)
            del self.cache_entries[key]
            logger.info(f"Evicted cache entry: {key}")
    
    def _create_cache_key(self, table_id: str, partition_spec: Dict, columns: Set[str]) -> CacheKey:
        """Create cache key from parameters"""
        partition_str = json.dumps(partition_spec, sort_keys=True) if partition_spec else "{}"
        return CacheKey(table_id, partition_str, columns)

    def _ensure_memory_available(self, required_bytes: int):
        """Ensure sufficient memory is available, evicting if necessary"""
        if self.memory_manager.allocate(required_bytes):
            return
        
        # Need to evict some entries
        current_usage = self.memory_manager.allocated_bytes
        target_free = required_bytes
        
        keys_to_evict = self.current_policy.should_evict(self.cache_entries, target_free)
        
        for key in keys_to_evict:
            self._evict_entry(key)
            if self.memory_manager.allocate(required_bytes):
                return
        
        raise MemoryError(f"Cannot allocate {required_bytes} bytes even after eviction")

    # def load_table_partition(self, table_location: str, partition_values: Dict[str, Any] = None, 
    #                        columns: List[str] = None) -> pa.RecordBatch:
    #     """Load a table partition into cache"""
    #     cache_key = self._create_cache_key(table_location, partition_values, columns)
        
    #     with self._lock:
    #         # Check if already cached
    #         if cache_key in self.cache_entries:
    #             entry = self.cache_entries[cache_key]
    #             entry.touch()
    #             return entry.data
            
    #         # Load metadata
    #         metadata = self.metadata_manager.read_table_metadata(table_location)
            
    #         # Get partition info
    #         if table_location not in self.partition_info_cache:
    #             self.partition_info_cache[table_location] = \
    #                 self.metadata_manager.get_partition_info(table_location)
            
    #         # Find matching partition
    #         partition_info = self.partition_info_cache[table_location][0]  # Simplified
            
    #         # Load data
    #         batch = self.data_loader.load_parquet_file(partition_info.file_path, columns)
            
    #         # Calculate memory required
    #         batch_size = batch.nbytes
            
    #         # Ensure memory available
    #         self._ensure_memory_available(batch_size)
            
    #         # Create cache entry
    #         entry = CacheEntry(
    #             key=cache_key,
    #             data=batch,
    #             size_bytes=batch_size,
    #             partition_info=partition_values
    #         )
            
    #         self.cache_entries[cache_key] = entry
            
    #         # Build indices
    #         self._build_indices(cache_key, batch)
            
    #         return batch
    
    def get_table_data(self, table_id: str, 
                      partition_filter: Optional[Dict] = None,
                      columns: Optional[List[str]] = None) -> pa.Table:
        """Get table data with caching"""
        
        # Create cache key
        cols_set = set(columns) if columns else set()
        cache_key = self._create_cache_key(table_id, partition_filter or {}, cols_set)
        cache_key_str = str(cache_key)
        
        # Try cache first
        cached_table = self.cache.get(cache_key_str)
        if cached_table is not None:
            return cached_table
        
        # Load from Iceberg/S3
        try:
            # Get file list from Iceberg
            files = self.metadata_manager.get_data_files(table_id, partition_filter)
            if not files:
                return pa.table({})  # Empty table
            
            # Load data
            table = self.data_loader.load_multiple_parquet_files(
                [file.file_path for file in files], 
                columns
            )
            
            # Cache the result
            self.cache.put(cache_key_str, table)
            
            return table
            
        except Exception as e:
            logger.error(f"Failed to load table data: {e}")
            raise
    
    def execute_query(self, sql: str) -> pa.Table:
        """Execute simple SQL queries (MVP implementation)"""
        # This is a very basic SQL parser for MVP
        # In production, you'd use a proper SQL engine like DuckDB or DataFusion
        
        sql = sql.strip().upper()
        
        if not sql.startswith('SELECT'):
            raise ValueError("Only SELECT queries supported in MVP")
        
        # Very basic parsing - extract table name
        # Format: SELECT * FROM table_name [WHERE conditions]
        parts = sql.split()
        if 'FROM' not in parts:
            raise ValueError("FROM clause required")
        
        from_idx = parts.index('FROM')
        table_id = parts[from_idx + 1].lower()
        
        # Load full table for MVP (no predicate pushdown yet)
        return self.get_table_data(table_id)

    def _build_indices(self, key: str, batch: pa.RecordBatch):
        """Build index structures for a batch"""
        if self.enable_bloom_filters:
            bloom_filter = BloomFilter()
            
            # Add string columns to bloom filter
            for col_name in batch.column_names:
                column = batch[col_name]
                if pa.types.is_string(column.type):
                    for val in column.to_pylist():
                        if val is not None:
                            bloom_filter.add(str(val))
            
            self.bloom_filters[key] = bloom_filter
        
        # Build min/max statistics
        stats = {}
        for col_name in batch.column_names:
            column = batch[col_name]
            if (pa.types.is_integer(column.type) or 
                pa.types.is_floating(column.type) or
                pa.types.is_decimal(column.type) or
                pa.types.is_date(column.type) or
                pa.types.is_timestamp(column.type)):
                try:
                    min_val = pc.min(column).as_py()
                    max_val = pc.max(column).as_py()
                    stats[col_name] = (min_val, max_val)
                except Exception:
                    pass
        
        self.min_max_stats[key] = stats
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return self.cache.get_stats()

    def invalidate_table(self, table_id: str) -> None:
        """Invalidate all cached entries for a table"""
        keys_to_remove = []
        for key in self.cache.cache.keys():
            parts = key.split("#")
            if len(parts) == 3 and parts[0] == table_id:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            with self.cache.lock:
                if key in self.cache.cache:
                    entry = self.cache.cache[key]
                    self.cache.current_size_bytes -= entry.size_bytes
                    del self.cache.cache[key]
        
        logger.info(f"Invalidated {len(keys_to_remove)} entries for table {table_id}")
