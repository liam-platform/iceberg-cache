"""Stress scenario: baseline → sudden 8× load spike → recovery.

Probes whether the system survives a traffic surge and whether throughput
returns to baseline once the surge subsides.
"""

import random
import threading
import time
from typing import Tuple

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 256 * 1024       # 256 KB
_CACHE_BYTES = 20 * _TABLE_BYTES  # 5 MB — tight, evictions happen
_N_TABLES = 100                   # larger pool than cache can hold
_PHASE_DURATION_S = 3.0

_BASELINE_THREADS = 4
_SPIKE_THREADS = 32              # 8× baseline — the "traffic surge"


def _run_phase(
    cache: LRUCache,
    tables: list,
    keys: list,
    n_threads: int,
    duration_s: float,
    write_ratio: float,
    seed: int,
) -> Tuple[float, int]:
    """Run a timed phase, return (ops_per_second, unhandled_error_count)."""
    stop = threading.Event()
    total_ops = [0]
    total_errors = [0]
    merge_lock = threading.Lock()

    def worker(w_seed: int) -> None:
        rng = random.Random(w_seed)
        local_ops = 0
        local_errors = 0
        while not stop.is_set():
            idx = rng.randint(0, len(keys) - 1)
            try:
                if rng.random() < write_ratio:
                    cache.put(keys[idx], tables[idx])
                else:
                    cache.get(keys[idx])
                local_ops += 1
            except MemoryError:
                pass  # expected under pressure — not an error
            except Exception:
                local_errors += 1
        with merge_lock:
            total_ops[0] += local_ops
            total_errors[0] += local_errors

    threads = [
        threading.Thread(target=worker, args=(seed + i,), daemon=True)
        for i in range(n_threads)
    ]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_s)
    stop.set()
    for t in threads:
        t.join(timeout=5)
    elapsed = time.perf_counter() - t_start

    return total_ops[0] / elapsed, total_errors[0]


class SpikeTestScenario(BaseScenario):
    """Simulate an 8× thread-count traffic surge then verify recovery.

    Phases:
      1. Baseline  — 4 threads, 25% writes, 3 s → measure stable throughput
      2. Spike     — 32 threads, 70% writes, 3 s → maximum eviction pressure
      3. Recovery  — 4 threads, 25% writes, 3 s → must return to ≥ 70% baseline

    Breaking point being probed: does eviction thrash under spike cause the
    cache to deadlock, corrupt size accounting, or fail to recover?
    """

    name = "spike_test"
    category = "stress"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())
        tables = [make_table_sized(_TABLE_BYTES, seed=i + 300) for i in range(_N_TABLES)]
        keys = [f"spike_{i}#{{}}_#surge" for i in range(_N_TABLES)]

        # Pre-warm so baseline reads have hits
        for i in range(_CACHE_BYTES // _TABLE_BYTES):
            try:
                cache.put(keys[i], tables[i])
            except MemoryError:
                pass

        with Timer() as wall:
            baseline_ops, e1 = _run_phase(
                cache, tables, keys, _BASELINE_THREADS, _PHASE_DURATION_S,
                write_ratio=0.25, seed=0,
            )
            spike_ops, e2 = _run_phase(
                cache, tables, keys, _SPIKE_THREADS, _PHASE_DURATION_S,
                write_ratio=0.70, seed=1000,
            )
            recovery_ops, e3 = _run_phase(
                cache, tables, keys, _BASELINE_THREADS, _PHASE_DURATION_S,
                write_ratio=0.25, seed=2000,
            )

        total_errors = e1 + e2 + e3
        within_budget = cache.current_size_bytes <= _CACHE_BYTES
        recovery_ratio = recovery_ops / baseline_ops if baseline_ops > 0 else 0.0
        spike_ratio = spike_ops / baseline_ops if baseline_ops > 0 else 0.0

        passed = within_budget and total_errors == 0 and recovery_ratio >= 0.70

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=wall.elapsed_s,
            ops_per_sec=baseline_ops,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            cache_hit_rate=0.0,
            evictions=0,
            errors=total_errors,
            summary=(
                f"baseline={baseline_ops:>9,.0f} ops/s  "
                f"spike={spike_ops:>9,.0f} ({spike_ratio*100:.0f}% of baseline)  "
                f"recovery={recovery_ops:>9,.0f} ({recovery_ratio*100:.0f}%)  "
                f"budget_ok={within_budget}"
            ),
        )
