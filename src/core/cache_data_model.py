import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Set

import pyarrow as pa


class CachePolicy(Enum):
    LRU = "lru"
    LFU = "lfu"
    CUSTOM = "custom"


@dataclass
class PartitionInfo:
    """Partition information for pruning"""
    partition_values: Dict[str, Any]
    file_path: str
    row_count: int
    file_size_bytes: int
    min_values: Dict[str, Any]
    max_values: Dict[str, Any]


@dataclass
class CacheKey:
    """Represents a cache key for a table partition."""
    table_id: str
    partition_spec: str
    columns: Set[str]

    def __str__(self) -> str:
        cols_hash = hashlib.md5(str(sorted(self.columns)).encode()).hexdigest()[:8]
        return f"{self.table_id}#{self.partition_spec}#{cols_hash}"


@dataclass
class CacheEntry:
    """CacheEntry contains Arrow table and metadata"""
    table: pa.Table
    timestamp: float
    size_bytes: int
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    partition_info: Optional[Dict[str, Any]] = None

    def touch(self):
        """Update access metadata"""
        self.access_count += 1
        self.last_accessed = time.time()
