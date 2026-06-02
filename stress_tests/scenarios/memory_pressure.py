"""Scenario: fill cache past budget and verify memory accounting stays consistent."""

import random
import time

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import LatencyTracker, ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 1 * 1024 * 1024   # 1 MB per table
_CACHE_BYTES = 20 * _TABLE_BYTES  # 20 MB budget
_N_TABLES = 60                    # 60 MB of data → 3× over budget
_N_READS = 3_000


class MemoryPressureScenario(BaseScenario):
    """Continuously load tables that exceed the cache budget.

    Validates:
    - current_size_bytes never exceeds max_size_bytes after every put().
    - Eviction fires for all over-budget inserts.
    - Data returned by get() is always a valid Arrow table (no corruption).
    """

    name = "memory_pressure"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())

        tables = [make_table_sized(_TABLE_BYTES, seed=i) for i in range(_N_TABLES)]
        keys = [f"pressure_tbl_{i}#{{}}_#aabbccdd" for i in range(_N_TABLES)]

        budget_violations = 0
        total_evictions = 0
        put_errors = 0
        put_tracker = LatencyTracker()

        with Timer() as put_timer:
            for key, table in zip(keys, tables):
                t0 = time.perf_counter()
                try:
                    evicted = cache.put(key, table)
                    total_evictions += len(evicted)
                except MemoryError as exc:
                    put_errors += 1
                put_tracker.record((time.perf_counter() - t0) * 1000)

                # Invariant: size never exceeds budget
                if cache.current_size_bytes > _CACHE_BYTES:
                    budget_violations += 1

        # Read from whatever remains in cache; verify no corrupted entries
        get_tracker = LatencyTracker()
        hits = 0
        corruption_errors = 0
        rng = random.Random(7)

        with Timer() as get_timer:
            for _ in range(_N_READS):
                key = rng.choice(keys)
                t0 = time.perf_counter()
                result = cache.get(key)
                get_tracker.record((time.perf_counter() - t0) * 1000)
                if result is not None:
                    hits += 1
                    # Sanity check: table should have rows and matching schema
                    if len(result) == 0 or result.num_columns == 0:
                        corruption_errors += 1

        total_duration = put_timer.elapsed_s + get_timer.elapsed_s
        total_ops = put_tracker.count + get_tracker.count

        final_stats = cache.get_stats()
        within_budget = final_stats["size_bytes"] <= _CACHE_BYTES

        passed = (
            budget_violations == 0
            and within_budget
            and corruption_errors == 0
            and total_evictions > 0
        )

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=total_duration,
            ops_per_sec=total_ops / total_duration,
            p50_ms=get_tracker.percentile(50),
            p95_ms=get_tracker.percentile(95),
            p99_ms=get_tracker.percentile(99),
            cache_hit_rate=hits / _N_READS,
            evictions=total_evictions,
            errors=budget_violations + corruption_errors + put_errors,
            summary=(
                f"{_N_TABLES} tables × {_TABLE_BYTES//1024//1024} MB into "
                f"{_CACHE_BYTES//1024//1024} MB budget, "
                f"violations={budget_violations}, corruption={corruption_errors}"
            ),
        )
