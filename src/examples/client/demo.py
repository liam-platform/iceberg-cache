#!/usr/bin/env python3
"""iceberg-cache performance demo.

Shows cold (cache-miss) vs warm (cache-hit) latency for both sample tables
and reports the speedup ratio.

Usage (server must already be running):
    python src/examples/client/demo.py [--host localhost] [--port 8815] [--runs N]
"""

import argparse
import statistics
import sys
import time

import pyarrow.flight as flight

from flight_client import CacheClient

# ANSI colour helpers
_BOLD  = "\033[1m"
_GREEN = "\033[32m"
_CYAN  = "\033[36m"
_DIM   = "\033[2m"
_RESET = "\033[0m"

def _c(text: str, *codes: str) -> str:
    if not sys.stdout.isatty():
        return text
    return "".join(codes) + text + _RESET


def _bar(ms: float, scale: float = 5.0) -> str:
    """Render a simple ASCII bar for a latency value."""
    filled = max(1, int(ms / scale))
    return "█" * min(filled, 60)


def _bench_table(client: CacheClient, table_id: str, warm_runs: int) -> None:
    print(f"\n  Table: {_c(table_id, _BOLD)}")

    # --- cold read (cache miss) ---
    print(f"    {'cold read':<20}", end="", flush=True)
    _, cold_ms = client.timed_query(table_id)
    print(f"  {cold_ms:>8.1f} ms  {_c(_bar(cold_ms), _DIM)}")

    # --- warm reads (cache hits) ---
    warm_latencies: list[float] = []
    for i in range(warm_runs):
        _, ms = client.timed_query(table_id)
        warm_latencies.append(ms)

    p50 = statistics.median(warm_latencies)
    p95 = sorted(warm_latencies)[int(len(warm_latencies) * 0.95)]
    speedup = cold_ms / p50 if p50 > 0 else float("inf")

    print(f"    {'cache hit p50':<20}  {p50:>8.1f} ms  {_c(_bar(p50), _GREEN)}")
    print(f"    {'cache hit p95':<20}  {p95:>8.1f} ms  {_c(_bar(p95), _GREEN)}")
    print(f"    {_c(f'speedup: {speedup:.0f}×', _CYAN)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="iceberg-cache demo")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8815)
    parser.add_argument("--runs", type=int, default=10,
                        help="Number of warm cache reads per table (default: 10)")
    args = parser.parse_args()

    location = f"grpc://{args.host}:{args.port}"

    print(f"\n{_c('iceberg-cache demo', _BOLD)}")
    print(f"Connecting to {location}...", end=" ", flush=True)

    try:
        client = CacheClient(location)
        tables = client.list_tables()
        print(f"OK  ({len(tables)} tables available)")
    except Exception as exc:
        print(f"\nFailed to connect: {exc}", file=sys.stderr)
        print("Make sure the server is running:  python src/main.py", file=sys.stderr)
        sys.exit(1)

    if not tables:
        print("\nNo tables found. Run the seed script first:")
        print("  python scripts/seed_data.py")
        sys.exit(1)

    print(f"\nAvailable tables: {', '.join(tables)}")
    print(f"Warm reads per table: {args.runs}")
    print("─" * 56)

    for table_id in tables:
        _bench_table(client, table_id, args.runs)

    print("\n" + "─" * 56)
    print(_c("Tip:", _BOLD), "cold read = S3 round-trip + Parquet decode.")
    print("      cache hit = Arrow in-memory copy, no I/O.\n")


if __name__ == "__main__":
    main()
