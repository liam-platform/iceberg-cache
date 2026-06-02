"""Scenario: raw put/get throughput — no external dependencies."""

import random
import time

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.config import DEFAULT_CACHE_BYTES
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import LatencyTracker, ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

# Cache holds 32 tables; we load 50 → causes ~18 evictions during the PUT phase.
_TABLE_BYTES = 512 * 1024       # 512 KB per table
_CACHE_BYTES = 32 * _TABLE_BYTES  # 16 MB budget → 32-table capacity
_N_TABLES = 50
_N_READS = 10_000


class CacheThroughputScenario(BaseScenario):
    """Measure LRUCache put/get throughput with pre-built Arrow tables.

    Validates: eviction triggers correctly, get latency stays sub-millisecond.
    """

    name = "cache_throughput"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())

        tables = [make_table_sized(_TABLE_BYTES, seed=i) for i in range(_N_TABLES)]
        keys = [f"tbl_{i}#{{}}_#deadbeef" for i in range(_N_TABLES)]

        # ── PUT phase ─────────────────────────────────────────────────
        put_tracker = LatencyTracker()
        total_evictions = 0
        errors = 0

        with Timer() as put_timer:
            for key, table in zip(keys, tables):
                t0 = time.perf_counter()
                try:
                    evicted = cache.put(key, table)
                    total_evictions += len(evicted)
                except MemoryError:
                    errors += 1
                put_tracker.record((time.perf_counter() - t0) * 1000)

        # ── GET phase ─────────────────────────────────────────────────
        get_tracker = LatencyTracker()
        hits = 0
        rng = random.Random(0)

        with Timer() as get_timer:
            for _ in range(_N_READS):
                key = rng.choice(keys)
                t0 = time.perf_counter()
                result = cache.get(key)
                get_tracker.record((time.perf_counter() - t0) * 1000)
                if result is not None:
                    hits += 1

        total_duration = put_timer.elapsed_s + get_timer.elapsed_s
        total_ops = put_tracker.count + get_tracker.count
        hit_rate = hits / _N_READS

        stats = cache.get_stats()
        within_budget = stats["size_bytes"] <= _CACHE_BYTES

        passed = (
            within_budget
            and total_evictions > 0          # eviction fired
            and hit_rate > 0.4               # reasonable hit rate
            and get_tracker.percentile(99) < 10  # p99 get < 10 ms
        )

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=total_duration,
            ops_per_sec=total_ops / total_duration,
            p50_ms=get_tracker.percentile(50),
            p95_ms=get_tracker.percentile(95),
            p99_ms=get_tracker.percentile(99),
            cache_hit_rate=hit_rate,
            evictions=total_evictions,
            errors=errors,
            summary=(
                f"PUT {_N_TABLES} tables ({_TABLE_BYTES//1024} KB each), "
                f"GET {_N_READS} random reads, "
                f"budget={_CACHE_BYTES//1024//1024} MB, within_budget={within_budget}"
            ),
        )
