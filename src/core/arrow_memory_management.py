import threading
from typing import Dict

import pyarrow as pa


class ArrowMemoryManager:
    """Manages Arrow memory pools and allocation"""
    
    def __init__(self, max_memory_bytes: int):
        self.max_memory_bytes = max_memory_bytes
        self.memory_pool = pa.default_memory_pool()
        self.allocated_bytes = 0
        self._lock = threading.Lock()
        
    def allocate(self, size_bytes: int) -> bool:
        """Try to allocate memory, return True if successful"""
        with self._lock:
            if self.allocated_bytes + size_bytes <= self.max_memory_bytes:
                self.allocated_bytes += size_bytes
                return True
            return False
    
    def deallocate(self, size_bytes: int):
        """Deallocate memory"""
        with self._lock:
            self.allocated_bytes = max(0, self.allocated_bytes - size_bytes)
    
    def get_memory_usage(self) -> Dict[str, int | float]:
        """Get current memory usage statistics"""
        return {
            "allocated_bytes": self.allocated_bytes,
            "max_memory_bytes": self.max_memory_bytes,
            "available_bytes": self.max_memory_bytes - self.allocated_bytes,
            "usage_percentage": (self.allocated_bytes / self.max_memory_bytes) * 100
        }
