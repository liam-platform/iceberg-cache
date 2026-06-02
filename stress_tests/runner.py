#!/usr/bin/env python3
"""Stress test runner for iceberg-cache.

Usage:
    python stress_tests/runner.py [options]

Options:
    --skip-docker       Skip scenarios that require Docker / MinIO
    --only NAME         Run only the named scenario
    --list              Print scenario names and exit
    --no-color          Disable ANSI color output
    --verbose           Show DEBUG/INFO log output from the cache internals
"""

import argparse
import logging
import socket
import sys
import time
from typing import List

from stress_tests.config import MINIO_HOST, MINIO_PORT
from stress_tests.metrics import ScenarioResult
from stress_tests.scenarios import (
    CacheThroughputScenario,
    ConcurrentAccessScenario,
    EvictionBenchmarkScenario,
    MemoryPressureScenario,
    MinioDataLoaderScenario,
)
from stress_tests.scenarios.base import BaseScenario

_SCENARIOS: List[BaseScenario] = [
    CacheThroughputScenario(),
    MemoryPressureScenario(),
    ConcurrentAccessScenario(),
    EvictionBenchmarkScenario(),
    MinioDataLoaderScenario(),
]

_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"
_BOLD = "\033[1m"


def _color(text: str, code: str, use_color: bool) -> str:
    return f"{code}{text}{_RESET}" if use_color else text


def _minio_reachable() -> bool:
    try:
        with socket.create_connection((MINIO_HOST, MINIO_PORT), timeout=2):
            return True
    except OSError:
        return False


def _run_scenario(scenario: BaseScenario, use_color: bool) -> ScenarioResult:
    print(f"  Running {scenario.name} ...", flush=True)
    try:
        scenario.setup()
        result = scenario.run()
    except Exception as exc:
        result = ScenarioResult(
            name=scenario.name,
            passed=False,
            duration_s=0.0,
            ops_per_sec=0.0,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            cache_hit_rate=0.0,
            evictions=0,
            errors=1,
            summary=f"EXCEPTION: {exc}",
        )
    finally:
        try:
            scenario.teardown()
        except Exception:
            pass
    return result


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="iceberg-cache stress tests")
    parser.add_argument("--skip-docker", action="store_true", help="Skip MinIO scenarios")
    parser.add_argument("--only", metavar="NAME", help="Run a single named scenario")
    parser.add_argument("--list", action="store_true", help="List scenario names and exit")
    parser.add_argument("--no-color", action="store_true", help="Disable color output")
    parser.add_argument("--verbose", action="store_true", help="Show cache INFO logs")
    args = parser.parse_args(argv)

    log_level = logging.INFO if args.verbose else logging.WARNING
    # basicConfig is a no-op if any handler already exists (dataloader.py configures
    # logging at import time), so force the level on the root logger directly.
    logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s")
    logging.getLogger().setLevel(log_level)

    use_color = not args.no_color and sys.stdout.isatty()

    if args.list:
        for s in _SCENARIOS:
            tag = " [docker]" if s.requires_docker else ""
            print(f"  {s.name}{tag}")
        return 0

    minio_up = _minio_reachable()

    to_run: List[BaseScenario] = []
    skipped: List[str] = []

    for scenario in _SCENARIOS:
        if args.only and scenario.name != args.only:
            continue
        if scenario.requires_docker:
            if args.skip_docker:
                skipped.append(f"{scenario.name} (--skip-docker)")
                continue
            if not minio_up:
                skipped.append(f"{scenario.name} (MinIO not reachable at {MINIO_HOST}:{MINIO_PORT})")
                continue
        to_run.append(scenario)

    if not to_run:
        print("No scenarios to run.")
        return 1

    print()
    print(_color(f"{'='*70}", _BOLD, use_color))
    print(_color("  iceberg-cache stress test suite", _BOLD, use_color))
    print(_color(f"{'='*70}", _BOLD, use_color))
    print()

    if skipped:
        for s in skipped:
            print(_color(f"  SKIP  {s}", _YELLOW, use_color))
        print()

    results: List[ScenarioResult] = []
    wall_start = time.perf_counter()

    for scenario in to_run:
        result = _run_scenario(scenario, use_color)
        results.append(result)

        status_color = _GREEN if result.passed else _RED
        status = "PASS" if result.passed else "FAIL"
        print(_color(f"  [{status}]", status_color, use_color), result.format_row()[8:])
        if result.summary:
            print(f"         {result.summary}")
        print()

    wall_elapsed = time.perf_counter() - wall_start

    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed

    print(_color(f"{'─'*70}", _BOLD, use_color))
    summary_color = _GREEN if failed == 0 else _RED
    print(
        _color(
            f"  {passed}/{len(results)} passed  "
            f"({len(skipped)} skipped)  "
            f"total={wall_elapsed:.1f}s",
            summary_color,
            use_color,
        )
    )
    print()

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
