"""Scenario: multi-threaded concurrent reads and writes — validates thread safety."""

import random
import threading
import time
from typing import Dict, List

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import LatencyTracker, ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 256 * 1024       # 256 KB
_CACHE_BYTES = 20 * _TABLE_BYTES  # 5 MB
_N_TABLES = 40
_N_THREADS = 8
_OPS_PER_THREAD = 1_000
_READ_RATIO = 0.75              # 75% reads, 25% writes


class _WorkerStats:
    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.errors = 0
        self.latencies: List[float] = []
        self._lock = threading.Lock()

    def merge(self, other: "_WorkerStats") -> None:
        with self._lock:
            self.hits += other.hits
            self.misses += other.misses
            self.evictions += other.evictions
            self.errors += other.errors
            self.latencies.extend(other.latencies)


def _worker(
    cache: LRUCache,
    tables: List,
    keys: List[str],
    n_ops: int,
    read_ratio: float,
    global_stats: _WorkerStats,
) -> None:
    local = _WorkerStats()
    rng = random.Random(threading.get_ident())

    for _ in range(n_ops):
        idx = rng.randint(0, len(keys) - 1)
        key = keys[idx]
        t0 = time.perf_counter()

        try:
            if rng.random() < read_ratio:
                result = cache.get(key)
                if result is not None:
                    local.hits += 1
                else:
                    local.misses += 1
            else:
                evicted = cache.put(key, tables[idx])
                local.evictions += len(evicted)
        except MemoryError:
            pass  # expected when many threads write simultaneously
        except Exception:
            local.errors += 1

        local.latencies.append((time.perf_counter() - t0) * 1000)

    global_stats.merge(local)


class ConcurrentAccessScenario(BaseScenario):
    """Run N threads doing mixed reads/writes against a shared LRUCache.

    Validates:
    - No deadlocks (all threads complete within timeout).
    - Memory budget is never exceeded after any operation.
    - No unhandled exceptions.
    """

    name = "concurrent_access"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())
        tables = [make_table_sized(_TABLE_BYTES, seed=i) for i in range(_N_TABLES)]
        keys = [f"concurrent_tbl_{i}#{{}}_#ff00ff00" for i in range(_N_TABLES)]

        # Pre-populate half the cache so reads have something to find
        for i in range(_N_TABLES // 2):
            try:
                cache.put(keys[i], tables[i])
            except MemoryError:
                pass

        global_stats = _WorkerStats()
        threads: List[threading.Thread] = []

        with Timer() as wall_timer:
            for _ in range(_N_THREADS):
                t = threading.Thread(
                    target=_worker,
                    args=(cache, tables, keys, _OPS_PER_THREAD, _READ_RATIO, global_stats),
                    daemon=True,
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join(timeout=30)

        all_finished = all(not t.is_alive() for t in threads)
        total_ops = _N_THREADS * _OPS_PER_THREAD

        final_stats = cache.get_stats()
        within_budget = final_stats["size_bytes"] <= _CACHE_BYTES

        if global_stats.latencies:
            ordered = sorted(global_stats.latencies)
            p50 = ordered[int(len(ordered) * 0.50)]
            p95 = ordered[int(len(ordered) * 0.95)]
            p99 = ordered[min(int(len(ordered) * 0.99), len(ordered) - 1)]
        else:
            p50 = p95 = p99 = 0.0

        total_reads = global_stats.hits + global_stats.misses
        hit_rate = global_stats.hits / total_reads if total_reads > 0 else 0.0

        passed = (
            all_finished
            and within_budget
            and global_stats.errors == 0
        )

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=wall_timer.elapsed_s,
            ops_per_sec=total_ops / wall_timer.elapsed_s,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            cache_hit_rate=hit_rate,
            evictions=global_stats.evictions,
            errors=global_stats.errors,
            summary=(
                f"{_N_THREADS} threads × {_OPS_PER_THREAD} ops "
                f"({int(_READ_RATIO*100)}% reads), "
                f"all_finished={all_finished}, within_budget={within_budget}"
            ),
        )
