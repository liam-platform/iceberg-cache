"""Scenario: compare hit rates of LRU vs LFU vs Custom under a skewed access pattern.

Loading order matters:
  1. Load hot tables only → warm them up with many reads (builds frequency).
  2. Flood with cold tables → every insert beyond budget forces an eviction.
  3. Test reads — 80% target hot keys, 20% cold.

LFU sees hot tables with access_count >> 1 and keeps them.
LRU sees cold inserts as the most-recent events and evicts the hot tables.
Custom (age + freq + size composite) should track LFU behavior here.
"""

import random
from typing import Dict, List, Tuple

from core.eviction_policy import CustomEvictionPolicy, LFUEvictionPolicy, LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 256 * 1024    # 256 KB per table
_HOT_COUNT = 15              # tables 0..14 are "hot"
_COLD_COUNT = 60             # tables 15..74 are "cold"
_N_TABLES = _HOT_COUNT + _COLD_COUNT
_CACHE_CAPACITY = 25         # fits 25 tables — smaller than hot + all cold
_CACHE_BYTES = _CACHE_CAPACITY * _TABLE_BYTES
_WARM_UP_ACCESSES = 40       # access each hot table this many times before cold flood
_N_TEST_OPS = 4_000
_HOT_PROBABILITY = 0.80      # 80% reads target hot tables in test phase


def _build_cache(policy) -> LRUCache:
    return LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=policy)


def _run_policy(
    policy,
    tables: List,
    keys: List[str],
    hot_indices: List[int],
    cold_indices: List[int],
    seed: int,
) -> Tuple[float, int]:
    """Return (hit_rate, eviction_count) for one policy."""
    cache = _build_cache(policy)
    rng = random.Random(seed)
    evictions = 0

    # Phase 1: seed cache with hot tables only
    for idx in hot_indices:
        evicted = cache.put(keys[idx], tables[idx])
        evictions += len(evicted)

    # Phase 2: read hot tables many times → builds high access_count
    for idx in hot_indices:
        for _ in range(_WARM_UP_ACCESSES):
            cache.get(keys[idx])

    # Phase 3: flood with cold tables — every insert past capacity evicts something
    for idx in cold_indices:
        try:
            evicted = cache.put(keys[idx], tables[idx])
            evictions += len(evicted)
        except MemoryError:
            pass

    # Phase 4: skewed reads
    hits = 0
    for _ in range(_N_TEST_OPS):
        if rng.random() < _HOT_PROBABILITY:
            idx = rng.choice(hot_indices)
        else:
            idx = rng.choice(cold_indices)
        if cache.get(keys[idx]) is not None:
            hits += 1

    return hits / _N_TEST_OPS, evictions


class EvictionBenchmarkScenario(BaseScenario):
    """Compare LRU / LFU / Custom eviction hit rates under a hot/cold workload.

    Hot set (15 tables) is warmed before cold flood begins.
    Expected outcome: LFU ≥ Custom > LRU because LFU/Custom protect high-frequency
    entries, while LRU evicts hot tables once cold inserts become the most-recent events.
    """

    name = "eviction_benchmark"
    requires_docker = False

    def run(self) -> ScenarioResult:
        tables = [make_table_sized(_TABLE_BYTES, seed=i + 100) for i in range(_N_TABLES)]
        keys = [f"bench_tbl_{i}#{{}}_#evictme" for i in range(_N_TABLES)]
        hot_indices = list(range(_HOT_COUNT))
        cold_indices = list(range(_HOT_COUNT, _N_TABLES))

        policies = {
            "LRU": LRUEvictionPolicy(),
            "LFU": LFUEvictionPolicy(),
            "Custom": CustomEvictionPolicy(),
        }
        results: Dict[str, Tuple[float, int]] = {}

        with Timer() as wall_timer:
            for name, policy in policies.items():
                hit_rate, evict_count = _run_policy(
                    policy, tables, keys, hot_indices, cold_indices, seed=99
                )
                results[name] = (hit_rate, evict_count)

        lru_hr = results["LRU"][0]
        lfu_hr = results["LFU"][0]
        custom_hr = results["Custom"][0]
        best_policy = max(results, key=lambda k: results[k][0])

        lines = [
            f"{name}: hit={hr*100:.1f}%"
            for name, (hr, _) in results.items()
        ]
        summary = "  |  ".join(lines) + f"  |  winner={best_policy}"

        # LFU should capture the hot set; must beat LRU on this frequency-skewed pattern
        passed = lfu_hr >= 0.55 and lfu_hr > lru_hr

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=wall_timer.elapsed_s,
            ops_per_sec=(_N_TEST_OPS * len(policies)) / wall_timer.elapsed_s,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            cache_hit_rate=lru_hr,
            evictions=sum(ec for _, ec in results.values()),
            errors=0,
            summary=summary,
        )
