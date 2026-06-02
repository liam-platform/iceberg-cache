#!/usr/bin/env python3
"""Stress test runner for iceberg-cache.

Two categories of scenarios:

  CORRECTNESS  Cache invariants must hold. Any failure is a bug.
  STRESS       Designed to find breaking points — the question is not
               "did it break?" but "how did it break and can it recover?".
               Graceful failure (MemoryError, throughput degradation within
               tolerance, clean recovery) is a passing outcome.

Usage:
    python stress_tests/runner.py [options]

Options:
    --skip-docker   Skip scenarios that require MinIO
    --only NAME     Run one named scenario
    --list          Print scenario names and exit
    --no-color      Plain text output (useful in CI)
    --verbose       Show INFO-level logs from cache internals
"""

import argparse
import logging
import socket
import sys
import time
from typing import List, Tuple

from stress_tests.config import MINIO_HOST, MINIO_PORT
from stress_tests.metrics import ScenarioResult
from stress_tests.scenarios import (
    CacheThrashScenario,
    CacheThroughputScenario,
    ConcurrentAccessScenario,
    EvictionBenchmarkScenario,
    MemoryPressureScenario,
    MinioDataLoaderScenario,
    OversizeEntryScenario,
    SoakTestScenario,
    SpikeTestScenario,
)
from stress_tests.scenarios.base import BaseScenario

# ── Scenario registry ──────────────────────────────────────────────────────────

_CORRECTNESS: List[BaseScenario] = [
    CacheThroughputScenario(),
    MemoryPressureScenario(),
    ConcurrentAccessScenario(),
    EvictionBenchmarkScenario(),
    MinioDataLoaderScenario(),
]

_STRESS: List[BaseScenario] = [
    SpikeTestScenario(),
    SoakTestScenario(),
    OversizeEntryScenario(),
    CacheThrashScenario(),
]

# ── ANSI helpers ───────────────────────────────────────────────────────────────

_GREEN  = "\033[32m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_CYAN   = "\033[36m"
_BOLD   = "\033[1m"
_DIM    = "\033[2m"
_RESET  = "\033[0m"


def _c(text: str, *codes: str, use_color: bool = True) -> str:
    if not use_color:
        return text
    return "".join(codes) + text + _RESET


# ── Utilities ──────────────────────────────────────────────────────────────────

def _minio_reachable() -> bool:
    try:
        with socket.create_connection((MINIO_HOST, MINIO_PORT), timeout=2):
            return True
    except OSError:
        return False


def _run_one(scenario: BaseScenario, use_color: bool) -> ScenarioResult:
    print(f"  {_c('→', _DIM, use_color=use_color)} {scenario.name} ...", flush=True)
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
            summary=f"UNHANDLED EXCEPTION: {exc}",
        )
    finally:
        try:
            scenario.teardown()
        except Exception:
            pass
    return result


def _print_result(result: ScenarioResult, use_color: bool) -> None:
    if result.passed:
        tag = _c("[PASS]", _GREEN, _BOLD, use_color=use_color)
    else:
        tag = _c("[FAIL]", _RED, _BOLD, use_color=use_color)

    # strip the leading "  [TAG]" that format_row includes so we control it
    row_body = result.format_row()[9:]
    print(f"  {tag}  {row_body}")
    if result.summary:
        print(f"  {_c(result.summary, _DIM, use_color=use_color)}")
    print()


def _section(title: str, subtitle: str, use_color: bool) -> None:
    print(_c(f"\n  {title}", _BOLD, use_color=use_color))
    print(_c(f"  {subtitle}", _DIM, use_color=use_color))
    print(_c("  " + "─" * 68, _DIM, use_color=use_color))
    print()


def _filter(
    scenarios: List[BaseScenario],
    skip_docker: bool,
    only: str | None,
    minio_up: bool,
) -> Tuple[List[BaseScenario], List[str]]:
    selected, skipped = [], []
    for s in scenarios:
        if only and s.name != only:
            continue
        if s.requires_docker:
            if skip_docker:
                skipped.append(f"{s.name}  (--skip-docker)")
                continue
            if not minio_up:
                skipped.append(f"{s.name}  (MinIO not reachable at {MINIO_HOST}:{MINIO_PORT})")
                continue
        selected.append(s)
    return selected, skipped


# ── Main ───────────────────────────────────────────────────────────────────────

def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="iceberg-cache stress tests")
    parser.add_argument("--skip-docker", action="store_true")
    parser.add_argument("--only", metavar="NAME")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s")
    logging.getLogger().setLevel(log_level)

    use_color = not args.no_color and sys.stdout.isatty()

    all_scenarios = _CORRECTNESS + _STRESS

    if args.list:
        for s in all_scenarios:
            docker_tag = "  [docker]" if s.requires_docker else ""
            print(f"  {s.name:<36}  [{s.category}]{docker_tag}")
        return 0

    minio_up = _minio_reachable()

    correctness, c_skipped = _filter(_CORRECTNESS, args.skip_docker, args.only, minio_up)
    stress, s_skipped = _filter(_STRESS, args.skip_docker, args.only, minio_up)
    all_skipped = c_skipped + s_skipped

    if not correctness and not stress:
        print("No scenarios matched.")
        return 1

    # ── Header ────────────────────────────────────────────────────────────────
    print()
    print(_c("=" * 70, _BOLD, use_color=use_color))
    print(_c("  iceberg-cache stress test suite", _BOLD, use_color=use_color))
    print(_c("=" * 70, _BOLD, use_color=use_color))

    if all_skipped:
        print()
        for s in all_skipped:
            print(_c(f"  SKIP  {s}", _YELLOW, use_color=use_color))

    # ── Correctness block ─────────────────────────────────────────────────────
    c_results: List[ScenarioResult] = []
    if correctness:
        _section(
            "CORRECTNESS  —  invariants must hold",
            "Any failure here is a bug.",
            use_color,
        )
        for scenario in correctness:
            result = _run_one(scenario, use_color)
            c_results.append(result)
            _print_result(result, use_color)

    # ── Stress block ──────────────────────────────────────────────────────────
    s_results: List[ScenarioResult] = []
    if stress:
        _section(
            "STRESS  —  find the breaking point",
            "Graceful failure, quantified degradation, and clean recovery are passing outcomes.",
            use_color,
        )
        for scenario in stress:
            result = _run_one(scenario, use_color)
            s_results.append(result)
            _print_result(result, use_color)

    # ── Summary ───────────────────────────────────────────────────────────────
    all_results = c_results + s_results
    passed = sum(1 for r in all_results if r.passed)
    failed = len(all_results) - passed
    wall_elapsed = sum(r.duration_s for r in all_results)

    print(_c("=" * 70, _BOLD, use_color=use_color))
    color = _GREEN if failed == 0 else _RED
    print(
        _c(
            f"  {passed}/{len(all_results)} passed"
            f"  ({len(all_skipped)} skipped)"
            f"  wall={wall_elapsed:.1f}s",
            color, _BOLD,
            use_color=use_color,
        )
    )
    if c_results:
        c_pass = sum(1 for r in c_results if r.passed)
        print(_c(f"  correctness  {c_pass}/{len(c_results)}", _DIM, use_color=use_color))
    if s_results:
        s_pass = sum(1 for r in s_results if r.passed)
        print(_c(f"  stress       {s_pass}/{len(s_results)}", _DIM, use_color=use_color))
    print()

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
