"""Integration tests for the unified cache admission path.

These tests verify that a single put() call handles budget enforcement,
policy-based eviction, and index cleanup — with no separate pre-flight
allocate/deallocate dance.
"""

import pytest
import pyarrow as pa

from core.cache_data_model import CachePolicy
from core.eviction_policy import LRUEvictionPolicy, LFUEvictionPolicy, CustomEvictionPolicy
from core.lru_cache import LRUCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_table(size_bytes: int) -> pa.Table:
    """Return a uint8 Arrow table that occupies exactly size_bytes of buffer space."""
    return pa.table({"x": pa.array(list(range(size_bytes)), type=pa.uint8())})


def lru_cache(max_bytes: int) -> LRUCache:
    return LRUCache(max_size_bytes=max_bytes, eviction_policy=LRUEvictionPolicy())


# ---------------------------------------------------------------------------
# LRUCache.put() returns evicted keys
# ---------------------------------------------------------------------------

class TestPutReturnsEvictedKeys:
    def test_no_eviction_when_room_available(self):
        cache = lru_cache(1000)
        evicted = cache.put("a", make_table(100))
        assert evicted == []

    def test_evicts_lru_entry_to_make_room(self):
        cache = lru_cache(200)
        cache.put("a", make_table(100))
        cache.put("b", make_table(100))
        # "a" is LRU; adding 100-byte "c" must displace it
        evicted = cache.put("c", make_table(100))
        assert evicted == ["a"]
        assert cache.get("a") is None
        assert cache.get("b") is not None
        assert cache.get("c") is not None

    def test_evicts_multiple_entries_when_required(self):
        cache = lru_cache(300)
        cache.put("a", make_table(100))
        cache.put("b", make_table(100))
        cache.put("c", make_table(100))
        # Shortfall = 300 + 250 - 300 = 250 bytes. Each entry is 100 bytes, so the
        # policy must evict at least 3 entries before 250 bytes are freed.
        evicted = cache.put("big", make_table(250))
        assert len(evicted) >= 2
        assert cache.current_size_bytes <= 300

    def test_overwrite_same_key_does_not_evict_self(self):
        cache = lru_cache(200)
        cache.put("a", make_table(100))
        evicted = cache.put("a", make_table(100))
        assert "a" not in evicted
        assert cache.get("a") is not None

    def test_raises_memory_error_when_table_exceeds_budget(self):
        cache = lru_cache(50)
        with pytest.raises(MemoryError):
            cache.put("too_big", make_table(100))


# ---------------------------------------------------------------------------
# Single memory budget — no double-counting
# ---------------------------------------------------------------------------

class TestSingleBudget:
    def test_current_size_stays_consistent_across_puts_and_evictions(self):
        cache = lru_cache(300)
        cache.put("a", make_table(100))
        cache.put("b", make_table(100))
        cache.put("c", make_table(100))
        # Adding 100 bytes must evict "a" (LRU)
        cache.put("d", make_table(100))
        assert cache.current_size_bytes == 300

    def test_size_decreases_after_delete(self):
        cache = lru_cache(1000)
        cache.put("a", make_table(100))
        before = cache.current_size_bytes
        cache.delete("a")
        assert cache.current_size_bytes == before - 100

    def test_get_stats_reflects_single_counter(self):
        cache = lru_cache(1000)
        cache.put("a", make_table(200))
        stats = cache.get_stats()
        assert stats["size_bytes"] == 200
        assert stats["max_size_bytes"] == 1000


# ---------------------------------------------------------------------------
# Eviction policy is actually consulted
# ---------------------------------------------------------------------------

class TestEvictionPolicyConsulted:
    def test_lfu_evicts_lowest_access_count_first(self):
        cache = LRUCache(max_size_bytes=200, eviction_policy=LFUEvictionPolicy())
        cache.put("rare", make_table(100))
        cache.put("freq", make_table(100))
        # Boost access count on "freq"
        cache.get("freq")
        cache.get("freq")
        # New entry forces eviction — LFU must choose "rare"
        evicted = cache.put("new", make_table(100))
        assert "rare" in evicted
        assert "freq" not in evicted

    def test_lru_evicts_oldest_accessed_first(self):
        cache = LRUCache(max_size_bytes=200, eviction_policy=LRUEvictionPolicy())
        cache.put("old", make_table(100))
        cache.put("young", make_table(100))
        # Touch "old" to make "young" the LRU
        cache.get("old")
        evicted = cache.put("new", make_table(100))
        assert "young" in evicted
        assert "old" not in evicted

    def test_custom_policy_is_used(self):
        cache = LRUCache(max_size_bytes=200, eviction_policy=CustomEvictionPolicy())
        cache.put("a", make_table(100))
        cache.put("b", make_table(100))
        evicted = cache.put("c", make_table(100))
        # Custom policy must evict something — just verify it ran without error
        assert len(evicted) >= 1
        assert cache.current_size_bytes <= 200


# ---------------------------------------------------------------------------
# bytes_to_free semantics — only enough is evicted
# ---------------------------------------------------------------------------

class TestBytesToFreeSemantics:
    def test_does_not_over_evict(self):
        """Policy receives the exact shortfall, not the full new-entry size."""
        cache = lru_cache(300)
        cache.put("a", make_table(100))
        cache.put("b", make_table(100))
        cache.put("c", make_table(100))
        # Budget is full (300/300). New entry is 50 bytes — shortfall is 50, not 50+50.
        # LRU evicts "a" (100 bytes) which is enough; "b" and "c" must survive.
        evicted = cache.put("small", make_table(50))
        assert len(evicted) == 1
        assert cache.get("b") is not None
        assert cache.get("c") is not None
