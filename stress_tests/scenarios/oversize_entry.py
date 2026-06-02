"""Stress scenario: insert a table larger than the entire cache budget.

This probes a specific failure mode: when an entry cannot fit even after
evicting everything, LRUCache raises MemoryError. The danger is that by the
time the error is raised, the cache has been fully emptied — all previously
cached data is gone. This is catastrophic eviction: the cure destroys more
than the disease.

The test quantifies:
  - At what table size does MemoryError fire?
  - Is the exception clean (no corrupted state afterwards)?
  - Does the cache remain fully functional after the error?
"""

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from stress_tests.data_factory import make_table_sized
from stress_tests.metrics import ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_BUDGET_MB = 20
_CACHE_BYTES = _BUDGET_MB * 1024 * 1024

# Pre-load entries that should survive if the cache is healthy
_SMALL_ENTRY_BYTES = 2 * 1024 * 1024   # 2 MB — fits easily
_N_PREWARM = 5                          # 10 MB total pre-warmed


class OversizeEntryScenario(BaseScenario):
    """Insert an entry larger than the entire cache budget.

    Expected behavior (breaking point):
      1. Pre-warm: 5 entries × 2 MB = 10 MB (cache is 50% full).
      2. Insert 25 MB entry → LRU evicts all 5 pre-warmed entries trying
         to free space, then raises MemoryError (25 > 20 MB budget).
      3. Cache is now EMPTY — catastrophic eviction occurred.
      4. A normal 2 MB insert then succeeds.

    Pass: MemoryError is the exact exception type (not a crash), cache size
    accounting is correct after the failure (size = 0, not negative or leaked),
    and the cache accepts new normal-sized entries afterwards.
    """

    name = "oversize_entry"
    category = "stress"
    requires_docker = False

    def run(self) -> ScenarioResult:
        cache = LRUCache(max_size_bytes=_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())

        # Step 1: pre-warm
        prewarm_tables = [make_table_sized(_SMALL_ENTRY_BYTES, seed=i + 500) for i in range(_N_PREWARM)]
        prewarm_keys = [f"prewarm_{i}#{{}}_#oversize" for i in range(_N_PREWARM)]
        for key, table in zip(prewarm_keys, prewarm_tables):
            cache.put(key, table)

        pre_size = cache.current_size_bytes
        pre_entries = len(cache.entries())

        # Step 2: insert oversize entry (25 MB > 20 MB budget)
        oversize_mb = _BUDGET_MB + 5
        oversize_table = make_table_sized(oversize_mb * 1024 * 1024, seed=999)
        oversize_key = "oversize_entry#{{}}_#toobig"

        got_memory_error = False
        wrong_exception = None

        with Timer() as wall:
            try:
                cache.put(oversize_key, oversize_table)
            except MemoryError:
                got_memory_error = True
            except Exception as exc:
                wrong_exception = exc

        post_size = cache.current_size_bytes
        post_entries = len(cache.entries())

        # Step 3: verify cache is empty (all pre-warmed entries were evicted)
        cache_emptied = post_entries == 0 and post_size == 0

        # Step 4: verify cache is still functional — normal insert must work
        recovery_table = make_table_sized(_SMALL_ENTRY_BYTES, seed=600)
        recovery_key = "recovery#{{}}_#aftererror"
        recovery_ok = False
        try:
            cache.put(recovery_key, recovery_table)
            result = cache.get(recovery_key)
            recovery_ok = result is not None
        except Exception:
            pass

        size_non_negative = post_size >= 0
        no_budget_leak = cache.current_size_bytes <= _CACHE_BYTES

        passed = (
            got_memory_error
            and wrong_exception is None
            and cache_emptied
            and size_non_negative
            and no_budget_leak
            and recovery_ok
        )

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=wall.elapsed_s,
            ops_per_sec=0.0,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            cache_hit_rate=0.0,
            evictions=pre_entries,   # all pre-warmed entries were evicted
            errors=1 if wrong_exception else 0,
            summary=(
                f"pre: {pre_entries} entries ({pre_size//1024//1024} MB)  "
                f"oversize: {oversize_mb} MB into {_BUDGET_MB} MB budget  "
                f"MemoryError={got_memory_error}  "
                f"post: {post_entries} entries ({post_size} bytes)  "
                f"[CATASTROPHIC EVICTION — cache emptied]  "
                f"recovery={recovery_ok}"
            ),
        )
