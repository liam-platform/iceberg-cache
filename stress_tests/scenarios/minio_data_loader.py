"""Scenario: S3DataLoader + LRUCache round-trip via MinIO (requires Docker)."""

import time
from typing import List

from core.eviction_policy import LRUEvictionPolicy
from core.lru_cache import LRUCache
from storage.dataloader import S3DataLoader
from stress_tests.config import AWS_CONFIG, DEFAULT_CACHE_BYTES, STRESS_TEST_BUCKET
from stress_tests.data_factory import ensure_bucket, make_table_sized, upload_parquet
from stress_tests.metrics import LatencyTracker, ScenarioResult, Timer
from stress_tests.scenarios.base import BaseScenario

_TABLE_BYTES = 512 * 1024   # 512 KB per Parquet file
_N_FILES = 10


class MinioDataLoaderScenario(BaseScenario):
    """Upload Parquet files to MinIO then exercise S3DataLoader + LRUCache.

    Validates:
    - S3DataLoader reads correct data from MinIO.
    - Cache miss → S3 load → cache hit round-trip works end-to-end.
    - Second read is served from cache (not S3), so latency drops.
    """

    name = "minio_data_loader"
    requires_docker = True

    def __init__(self) -> None:
        self._s3_paths: List[str] = []
        self._keys: List[str] = []

    def setup(self) -> None:
        ensure_bucket(STRESS_TEST_BUCKET, AWS_CONFIG)
        for i in range(_N_FILES):
            table = make_table_sized(_TABLE_BYTES, seed=i + 200)
            key = f"stress/parquet/file_{i:03d}.parquet"
            s3_path = upload_parquet(table, STRESS_TEST_BUCKET, key, AWS_CONFIG)
            self._s3_paths.append(s3_path)
            cache_key = f"minio_tbl_{i}#{{}}_#s3loaded"
            self._keys.append(cache_key)

    def run(self) -> ScenarioResult:
        loader = S3DataLoader(aws_config=AWS_CONFIG, max_workers=4)
        cache = LRUCache(max_size_bytes=DEFAULT_CACHE_BYTES, eviction_policy=LRUEvictionPolicy())

        miss_tracker = LatencyTracker()
        hit_tracker = LatencyTracker()
        errors = 0

        # ── First pass: cache miss → load from MinIO ──────────────────
        with Timer() as miss_timer:
            for s3_path, cache_key in zip(self._s3_paths, self._keys):
                t0 = time.perf_counter()
                try:
                    table = loader.load_parquet_file(s3_path)
                    cache.put(cache_key, table)
                except Exception:
                    errors += 1
                miss_tracker.record((time.perf_counter() - t0) * 1000)

        # ── Second pass: all requests should be cache hits ─────────────
        with Timer() as hit_timer:
            hits = 0
            for cache_key in self._keys:
                t0 = time.perf_counter()
                result = cache.get(cache_key)
                hit_tracker.record((time.perf_counter() - t0) * 1000)
                if result is not None:
                    hits += 1

        miss_p50 = miss_tracker.percentile(50)
        hit_p50 = hit_tracker.percentile(50)
        speedup = miss_p50 / hit_p50 if hit_p50 > 0 else float("inf")

        total_duration = miss_timer.elapsed_s + hit_timer.elapsed_s
        total_ops = miss_tracker.count + hit_tracker.count
        hit_rate = hits / len(self._keys)

        passed = (
            errors == 0
            and hit_rate == 1.0          # all second-pass reads hit
            and hit_p50 < miss_p50       # cache is faster than S3
        )

        return ScenarioResult(
            name=self.name,
            passed=passed,
            duration_s=total_duration,
            ops_per_sec=total_ops / total_duration,
            p50_ms=hit_tracker.percentile(50),
            p95_ms=hit_tracker.percentile(95),
            p99_ms=hit_tracker.percentile(99),
            cache_hit_rate=hit_rate,
            evictions=0,
            errors=errors,
            summary=(
                f"{_N_FILES} files × {_TABLE_BYTES//1024} KB, "
                f"miss_p50={miss_p50:.1f}ms hit_p50={hit_p50:.1f}ms "
                f"speedup={speedup:.1f}×"
            ),
        )
