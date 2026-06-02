"""Stress scenario: 100% eviction rate — every insert displaces an existing entry.

Cache thrash occurs when the working set is much larger than the cache capacity.
Every new table loads evicts something, making the eviction path the hot path
rather than the exception. This tests whether the cache degrades gracefully
(slower but functional) rather than catastrophically (corrupted state or crash).

This scenario is the worst-case operational pattern: an analytical workload
scanning tables sequentially with no temporal locality.
"""

import time

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import LatencyTracker, ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 512 * 1024   # 512 KB
_CACHE_CAPACITY = 4         # tiny cache — fits only 4 tables
_CACHE_BYTES = _CACHE_CAPACITY * _TABLE_BYTES
_N_TABLES = 200             # 50× cache capacity — guaranteed 100% eviction rate


class CacheThrashScenario(BaseScenario):
    """Sequential scan of 200 unique tables into a 4-table cache.

    Every insert from table #5 onwards evicts an entry. The eviction path
    (policy selection, index cleanup, size accounting) runs on every single
    operation — this is the worst-case load for those code paths.

    Breaking points being probed:
    - Does put() latency blow up as eviction overhead accumulates?
    - Does current_size_bytes stay correct after thousands of evictions?
    - Does the cache crash or corrupt data, or degrade gracefully?

    Pass: budget invariant holds throughout, no corruption on reads,
    put() p99 stays below 50 ms even at 100% eviction rate.
    """

    name = "cache_thrash"
    category = "stress"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())
        tables = [make_table_sized(_TABLE_BYTES, seed=i + 600) for i in range(_N_TABLES)]
        keys = [f"thrash_{i}#{{}}_#thrash" for i in range(_N_TABLES)]

        put_tracker = LatencyTracker()
        budget_violations = 0
        total_evictions = 0
        errors = 0

        with Timer() as wall:
            for key, table in zip(keys, tables):
                t0 = time.perf_counter()
                try:
                    evicted = cache.put(key, table)
                    total_evictions += len(evicted)
                except MemoryError:
                    errors += 1
                put_tracker.record((time.perf_counter() - t0) * 1000)

                if cache.current_size_bytes > _CACHE_BYTES:
                    budget_violations += 1

        # Read back the last _CACHE_CAPACITY entries — they should be in cache
        last_keys = keys[-_CACHE_CAPACITY:]
        hits = sum(1 for k in last_keys if cache.get(k) is not None)
        final_hit_rate = hits / _CACHE_CAPACITY

        # Verify data integrity on the ones that are cached
        corruption = 0
        for k in last_keys:
            t = cache.get(k)
            if t is not None and (len(t) == 0 or t.num_columns == 0):
                corruption += 1

        eviction_rate = total_evictions / _N_TABLES
        passed = (
            budget_violations == 0
            and corruption == 0
            and errors == 0
            and put_tracker.percentile(99) < 50
        )

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=wall.elapsed_s,
            ops_per_sec=_N_TABLES / wall.elapsed_s,
            p50_ms=put_tracker.percentile(50),
            p95_ms=put_tracker.percentile(95),
            p99_ms=put_tracker.percentile(99),
            cache_hit_rate=final_hit_rate,
            evictions=total_evictions,
            errors=budget_violations + corruption + errors,
            summary=(
                f"{_N_TABLES} sequential inserts into {_CACHE_CAPACITY}-table cache  "
                f"eviction_rate={eviction_rate*100:.0f}%  "
                f"violations={budget_violations}  corruption={corruption}"
            ),
        )
