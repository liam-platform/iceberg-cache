from typing import Dict

import pyarrow as pa


class ArrowMemoryManager:
    """Read-only diagnostics for the Arrow default memory pool."""

    def __init__(self, max_memory_bytes: int) -> None:
        self.max_memory_bytes = max_memory_bytes
        self._pool = pa.default_memory_pool()

    def get_memory_usage(self) -> Dict[str, int | float]:
        pool_bytes = self._pool.bytes_allocated
        return {
            "pool_bytes_allocated": pool_bytes,
            "max_memory_bytes": self.max_memory_bytes,
            "available_bytes": self.max_memory_bytes - pool_bytes,
            "usage_percentage": (pool_bytes / self.max_memory_bytes) * 100,
        }
