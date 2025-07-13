import logging
import threading
import time
from typing import Any, Dict, Optional, OrderedDict

import pyarrow as pa

from core.cache_data_model import CacheEntry
from core.cache_strategies import CacheStrategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LRUCache(CacheStrategy):
    """Baseline LRU Cache with size limits."""

    def __init__(self, max_size_bytes: int = 2 * 1024 * 1024 * 1024) -> None: # 2GB by default
        self.max_size_bytes = max_size_bytes
        self.current_size_bytes = 0
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Optional[pa.Table]:
        """Get item from cache, updating access time."""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                entry.access_count += 1
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                logger.info(f"Cache HIT for key {key}")
                return entry.table

            logger.info(f"Cache MISS for {key}")
            return None
    
    def put(self, key: str, table: pa.Table) -> None:
        """Put item in cache with eviction if needed."""
        size_bytes = table.nbytes
        with self.lock:
            # Remove existing entry if present
            if key in self.cache:
                old_entry = self.cache[key]
                self.current_size_bytes -= old_entry.size_bytes
                del self.cache[key]
            
            # Evict LRU items if needed
            while (self.current_size_bytes + size_bytes > self.max_size_bytes) and len(self.cache) > 0:
                lru_key, lru_entry = self.cache.popitem(last=False)
                self.current_size_bytes -= lru_entry.size_bytes
                logger.info(f"Evicted {lru_key} ({lru_entry.size_bytes} bytes)")
            
            # Add new entry
            entry = CacheEntry(
                table=table,
                timestamp=time.time(),
                access_count=1,
                size_bytes=size_bytes
            )
            self.cache[key] = entry
            self.current_size_bytes += size_bytes
            logger.info(f"Cached {key} ({size_bytes} bytes), total: {self.current_size_bytes}")
        
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.lock:
            return {
                "entries": len(self.cache),
                "size_bytes": self.current_size_bytes,
                "max_size_bytes": self.max_size_bytes,
                "utilization": self.current_size_bytes / self.max_size_bytes * 100
            }
