from dataclasses import dataclass
import hashlib

from typing import Set

import pyarrow as pa


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
    access_count: int
    size_bytes: int
