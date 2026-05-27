import logging
import threading
import time
from typing import Any, Dict, Optional, OrderedDict

import pyarrow as pa

from core.cache_data_model import CacheEntry
from core.cache_strategies import CacheStrategy

logger = logging.getLogger(__name__)


class LRUCache(CacheStrategy):
    """Baseline LRU Cache with size limits."""

    def __init__(self, max_size_bytes: int = 2 * 1024 * 1024 * 1024) -> None:  # 2GB by default
        self.max_size_bytes = max_size_bytes
        self.current_size_bytes = 0
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[pa.Table]:
        """Get item from cache, updating LRU order and access metadata."""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                entry.touch()
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                logger.info(f"Cache HIT for key {key}")
                return entry.table

            logger.info(f"Cache MISS for {key}")
            return None

    def get_entry(self, key: str) -> Optional[CacheEntry]:
        """Peek at a cache entry without updating LRU order.

        Use this when you need the full CacheEntry metadata (e.g. last_accessed,
        access_count) without side-effects on eviction ordering.
        """
        with self.lock:
            return self.cache.get(key)

    def put(self, key: str, table: pa.Table) -> None:
        """Put item in cache with LRU eviction if needed."""
        size_bytes = table.nbytes
        with self.lock:
            # Remove existing entry if present so we don't double-count
            if key in self.cache:
                old_entry = self.cache[key]
                self.current_size_bytes -= old_entry.size_bytes
                del self.cache[key]

            # Evict LRU items until there is room
            while (self.current_size_bytes + size_bytes > self.max_size_bytes) and len(self.cache) > 0:
                lru_key, lru_entry = self.cache.popitem(last=False)
                self.current_size_bytes -= lru_entry.size_bytes
                logger.info(f"Evicted {lru_key} ({lru_entry.size_bytes} bytes)")

            # Add new entry
            entry = CacheEntry(
                table=table,
                timestamp=time.time(),
                access_count=1,
                size_bytes=size_bytes,
            )
            self.cache[key] = entry
            self.current_size_bytes += size_bytes
            logger.info(f"Cached {key} ({size_bytes} bytes), total: {self.current_size_bytes}")

    def delete(self, key: str) -> Optional[int]:
        """Remove a single entry.

        Returns the number of bytes freed, or ``None`` if the key was not found.
        """
        with self.lock:
            if key in self.cache:
                entry = self.cache.pop(key)
                self.current_size_bytes -= entry.size_bytes
                logger.info(f"Deleted cache entry {key} ({entry.size_bytes} bytes)")
                return entry.size_bytes
            return None

    def invalidate_prefix(self, prefix: str) -> int:
        """Remove all entries whose key starts with *prefix*.

        Returns the number of entries removed.
        """
        with self.lock:
            keys_to_remove = [k for k in self.cache if k.startswith(prefix)]
            for key in keys_to_remove:
                entry = self.cache.pop(key)
                self.current_size_bytes -= entry.size_bytes
            if keys_to_remove:
                logger.info(f"Invalidated {len(keys_to_remove)} entries with prefix '{prefix}'")
            return len(keys_to_remove)

    def entries(self) -> Dict[str, CacheEntry]:
        """Return a shallow copy of all current cache entries.

        The copy is safe to iterate outside the lock while new entries may
        still be added or removed concurrently.
        """
        with self.lock:
            return dict(self.cache)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                "entries": len(self.cache),
                "size_bytes": self.current_size_bytes,
                "max_size_bytes": self.max_size_bytes,
                "utilization": self.current_size_bytes / self.max_size_bytes * 100,
            }
