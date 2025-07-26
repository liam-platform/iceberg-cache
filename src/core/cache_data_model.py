import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa


class CachePolicy(Enum):
    LRU = "lru"
    LFU = "lfu"
    CUSTOM = "custom"


class PartitionState(Enum):
    """States of partition data in the distributed cache"""
    NOT_CACHED = "not_cached"
    LOADING = "loading"
    CACHED_LOCAL = "cached_local"
    CACHED_REMOTE = "cached_remote"
    FAILED = "failed"


@dataclass
class PartitionInfo:
    """Represents partition metadata from Iceberg with distributed cache awareness"""
    partition_id: str
    table_name: str
    partition_spec: Dict[str, Any]
    file_path: str
    record_count: int
    file_size_bytes: int
    lower_bounds: Dict[str, Any]
    upper_bounds: Dict[str, Any]
    data_files: List[str] = field(default_factory=list)
    manifest_file: str = ""
    snapshot_id: int = 0
    
    def get_cache_key(self) -> str:
        """Generate unique cache key for this partition"""
        key_data = {
            'table': self.table_name,
            'partition_id': self.partition_id,
            'snapshot_id': self.snapshot_id,
            'manifest': self.manifest_file
        }
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()


@dataclass
class CacheLocation:
    """Represents where partition data is cached in the distributed system"""
    node_id: str
    node_address: str
    state: PartitionState
    cached_at: float
    access_count: int = 0
    last_accessed: float = 0.0
    memory_usage: int = 0


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
