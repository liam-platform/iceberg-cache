import time
from abc import ABC, abstractmethod
from typing import Dict, List

from core.cache_data_model import CacheEntry


class EvictionPolicy(ABC):
    """Abstract base class for cache eviction policies"""
    
    @abstractmethod
    def should_evict(self, entries: Dict[str, CacheEntry], target_size: int) -> List[str]:
        """Return list of keys to evict to reach target size"""
        pass


class LRUEvictionPolicy(EvictionPolicy):
    """Least Recently Used eviction policy"""
    
    def should_evict(self, entries: Dict[str, CacheEntry], target_size: int) -> List[str]:
        # Sort by last accessed time (oldest first)
        sorted_entries = sorted(entries.items(), key=lambda x: x[1].last_accessed)
        
        keys_to_evict = []
        freed_size = 0
        
        for key, entry in sorted_entries:
            if freed_size >= target_size:
                break
            keys_to_evict.append(key)
            freed_size += entry.size_bytes
            
        return keys_to_evict


class LFUEvictionPolicy(EvictionPolicy):
    """Least Frequently Used eviction policy"""
    
    def should_evict(self, entries: Dict[str, CacheEntry], target_size: int) -> List[str]:
        # Sort by access count (lowest first), then by last accessed
        sorted_entries = sorted(
            entries.items(), 
            key=lambda x: (x[1].access_count, x[1].last_accessed)
        )
        
        keys_to_evict = []
        freed_size = 0
        
        for key, entry in sorted_entries:
            if freed_size >= target_size:
                break
            keys_to_evict.append(key)
            freed_size += entry.size_bytes
            
        return keys_to_evict

class CustomEvictionPolicy(EvictionPolicy):
    """Custom eviction policy combining recency, frequency and size factors"""
    
    def should_evict(self, entries: Dict[str, CacheEntry], target_size: int) -> List[str]:
        # Calculate composite score: recency + frequency + size
        def calculate_score(entry: CacheEntry) -> float:
            age = time.time() - entry.last_accessed
            freq_score = 1.0 / (entry.access_count + 1)
            size_score = entry.size_bytes / (1024 * 1024)  # MB
            return age * 0.4 + freq_score * 0.4 + size_score * 0.2
        
        sorted_entries = sorted(
            entries.items(), 
            key=lambda x: calculate_score(x[1]), 
            reverse=True
        )
        
        keys_to_evict = []
        freed_size = 0
        
        for key, entry in sorted_entries:
            if freed_size >= target_size:
                break
            keys_to_evict.append(key)
            freed_size += entry.size_bytes
            
        return keys_to_evict
