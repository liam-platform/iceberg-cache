"""Stress scenario: sustained load for 20 seconds — detects throughput degradation
and memory-accounting drift over time.

Unlike a short burst test, soak testing surfaces problems that only appear after
many eviction cycles: leaked size bytes, index structures growing unbounded,
or lock-ordering issues that worsen under cumulative state.
"""

import random
import threading
import time
from typing import List, Tuple

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 256 * 1024
_CACHE_BYTES = 15 * _TABLE_BYTES   # 3.75 MB — tight
_N_TABLES = 80
_N_THREADS = 6
_SOAK_DURATION_S = 20
_SAMPLE_INTERVAL_S = 4             # 5 samples over 20 s


def _sample(
    cache: LRUCache,
    tables: list,
    keys: list,
    n_threads: int,
    duration_s: float,
    seed: int,
) -> Tuple[float, int, int]:
    """Run one measurement window. Returns (ops_per_s, errors, size_bytes_at_end)."""
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
                if rng.random() < 0.40:
                    cache.put(keys[idx], tables[idx])
                else:
                    cache.get(keys[idx])
                local_ops += 1
            except MemoryError:
                pass
            except Exception:
                local_errors += 1
        with merge_lock:
            total_ops[0] += local_ops
            total_errors[0] += local_errors

    threads = [
        threading.Thread(target=worker, args=(seed + i,), daemon=True)
        for i in range(n_threads)
    ]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_s)
    stop.set()
    for t in threads:
        t.join(timeout=5)
    elapsed = time.perf_counter() - t0

    return total_ops[0] / elapsed, total_errors[0], cache.current_size_bytes


class SoakTestScenario(BaseScenario):
    """Run sustained mixed load for 20 seconds, sampling throughput every 4 s.

    Breaking points being probed:
    - Throughput cliff: ops/s drops > 30% from first sample to last
      (indicates eviction overhead growing or lock contention worsening).
    - Memory drift: current_size_bytes creeps upward beyond budget
      (indicates size-accounting leak).
    - Cumulative errors: unhandled exceptions accumulate over time.

    The test prints a throughput timeline so degradation is visible even
    when the drop is below the failure threshold.
    """

    name = "soak_20s"
    category = "stress"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())
        tables = [make_table_sized(_TABLE_BYTES, seed=i + 400) for i in range(_N_TABLES)]
        keys = [f"soak_{i}#{{}}_#soak" for i in range(_N_TABLES)]

        # Pre-warm
        for i in range(_CACHE_BYTES // _TABLE_BYTES):
            try:
                cache.put(keys[i], tables[i])
            except MemoryError:
                pass

        samples: List[Tuple[float, int, int]] = []  # (ops/s, errors, size_bytes)

        with Timer() as wall:
            n_windows = _SOAK_DURATION_S // _SAMPLE_INTERVAL_S
            for window in range(n_windows):
                ops, errs, sz = _sample(
                    cache, tables, keys, _N_THREADS, _SAMPLE_INTERVAL_S,
                    seed=window * 1000,
                )
                samples.append((ops, errs, sz))

        ops_series = [s[0] for s in samples]
        total_errors = sum(s[1] for s in samples)
        max_size = max(s[2] for s in samples)

        first_ops = ops_series[0]
        last_ops = ops_series[-1]
        degradation = (first_ops - last_ops) / first_ops if first_ops > 0 else 0.0

        within_budget = max_size <= _CACHE_BYTES
        throughput_stable = degradation < 0.30   # allow up to 30% natural drift

        timeline = "  ".join(f"{ops/1000:.0f}K" for ops in ops_series)

        passed = within_budget and total_errors == 0 and throughput_stable

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=wall.elapsed_s,
            ops_per_sec=sum(ops_series) / len(ops_series),
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            cache_hit_rate=0.0,
            evictions=0,
            errors=total_errors,
            summary=(
                f"timeline(ops/s): [{timeline}]  "
                f"degradation={degradation*100:.1f}%  "
                f"budget_ok={within_budget}"
            ),
        )
