"""Latency and throughput tracking for stress test scenarios."""

import time
from dataclasses import dataclass
from typing import List


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    duration_s: float
    ops_per_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    cache_hit_rate: float
    evictions: int
    errors: int
    summary: str = ""

    def format_row(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"  [{status}]  {self.name:<36}"
            f"  {self.duration_s:>6.2f}s"
            f"  {self.ops_per_sec:>8,.0f} ops/s"
            f"  p50={_fmt_latency(self.p50_ms)}"
            f"  p99={_fmt_latency(self.p99_ms)}"
            f"  hit={self.cache_hit_rate*100:>4.0f}%"
            f"  evict={self.evictions}"
        )


def _fmt_latency(ms: float) -> str:
    """Format a latency value, switching to µs when sub-millisecond."""
    us = ms * 1000
    if us < 1.0:
        return "  <1µs"
    if ms < 1.0:
        return f"{us:>4.0f}µs"
    return f"{ms:>4.1f}ms"


class LatencyTracker:
    """Collect per-operation latencies and compute percentiles."""

    def __init__(self) -> None:
        self._samples: List[float] = []  # milliseconds

    def record(self, latency_ms: float) -> None:
        self._samples.append(latency_ms)

    def percentile(self, pct: float) -> float:
        if not self._samples:
            return 0.0
        ordered = sorted(self._samples)
        idx = min(int(len(ordered) * pct / 100), len(ordered) - 1)
        return ordered[idx]

    @property
    def count(self) -> int:
        return len(self._samples)


class Timer:
    """Context manager that records wall-clock elapsed time."""

    def __init__(self) -> None:
        self.elapsed_s: float = 0.0
        self._start: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self.elapsed_s = time.perf_counter() - self._start
