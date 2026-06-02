# Stress Tests

A repeatable stress test suite for `iceberg-cache` — run it after every new feature to catch regressions in throughput, memory accounting, thread safety, and eviction correctness.

## Requirements

| Dependency | Version | Notes |
|---|---|---|
| Python | ≥ 3.13 | Match the project's minimum |
| Docker Desktop | latest (Apple Silicon) | Only needed for MinIO / integration tests |
| uv | any | `uv sync` to install project deps |

> **No extra packages needed.** The stress tests use only dependencies already declared in `pyproject.toml` (`pyarrow`, `boto3`, `psutil`).

## Quick start

```bash
# Standalone scenarios only — no Docker, runs in ~2 s
./run_stress_test.sh --skip-docker

# Full suite — starts MinIO automatically, ~3 s total
./run_stress_test.sh

# Single scenario
./run_stress_test.sh --only eviction_benchmark
```

## Mac M4 (16 GB unified memory) — notes

The M4 chip's unified memory architecture gives the CPU and Arrow memory pool access to the same high-bandwidth pool (~100 GB/s). A few practical implications:

**Default cache budget is 64 MB.** This is intentionally small — it exercises eviction pressure without inflating RSS. On 16 GB you can raise it to stress-test at production scale:

```bash
./run_stress_test.sh --cache-mb 2048   # 2 GB cache
```

**Expected throughput on M4.** In-memory operations run in the millions of ops/second range. The numbers below are representative baselines from a 16 GB M4 Mac; significant regressions should be visible within the first run after a change.

| Scenario | Typical throughput | p99 latency |
|---|---|---|
| `cache_throughput` | ~1.7 M ops/s | < 1 ms |
| `memory_pressure` | ~1.3 M ops/s | < 1 ms |
| `concurrent_access` | ~300 K ops/s | < 1 ms |
| `eviction_benchmark` | ~1.5 M ops/s | — |
| `minio_data_loader` | ~500 ops/s (S3-bound) | — |

**Docker Desktop on Apple Silicon.** Use Docker Desktop ≥ 4.30 with the `Use Rosetta for x86_64/amd64 emulation` setting disabled — native ARM images are used for both `minio/minio` and `minio/mc`.

## All scenarios

### `cache_throughput` — standalone

Loads 50 Arrow tables (512 KB each) into a 16 MB cache, then performs 10 000 random reads.

**What it checks:**
- Eviction fires before the 28th insert (budget = 32 × 512 KB = 16 MB)
- `current_size_bytes` never exceeds the budget after every `put()`
- p99 `get()` latency stays below 10 ms
- Hit rate > 40% (cache holds ~28 of 50 tables at steady state)

**Pass criteria:** all four conditions above are met.

---

### `memory_pressure` — standalone

Loads 60 tables × 1 MB into a 20 MB budget (3× overload).

**What it checks:**
- `current_size_bytes ≤ max_size_bytes` is an invariant that must hold after **every single** `put()` call — not just at the end.
- Data returned by `get()` is never empty or schema-corrupted.
- Eviction fires for every table that does not fit.

**Pass criteria:** zero budget violations, zero data corruption across 3 000 reads.

---

### `concurrent_access` — standalone

Spawns 8 threads. Each thread performs 1 000 operations: 75% reads (`get`), 25% writes (`put`), targeting randomly chosen keys. Threads run concurrently against a single shared `LRUCache`.

**What it checks:**
- All 8 threads finish within a 30-second wall-clock timeout (no deadlocks).
- `current_size_bytes ≤ max_size_bytes` at the end (no double-counting under concurrent writes).
- Zero unhandled exceptions (expected `MemoryError` from `put()` under pressure is caught and counted as a valid outcome, not an error).

**Pass criteria:** all threads complete, within budget, zero unhandled exceptions.

---

### `eviction_benchmark` — standalone

Compares LRU, LFU, and Custom eviction policies under a hot/cold workload designed to show a clear policy difference.

**Setup:**
1. Load 15 *hot* tables into a cache sized for 25 entries.
2. Access each hot table 40 times to build a frequency signal.
3. Flood with 60 *cold* tables — every insert beyond 25 evicts something.
4. Run 4 000 reads: 80% target hot keys, 20% cold.

**Expected outcome:**

| Policy | Hit rate | Why |
|---|---|---|
| LFU | ~82% | Hot tables have `access_count = 40+`; cold tables `= 1`. LFU keeps hot. |
| Custom | ~82% | Composite score (age 40% + freq 40% + size 20%) also favours hot tables. |
| LRU | ~7% | Cold inserts are the most-recent events; LRU evicts hot tables to make room. |

This scenario demonstrates that **eviction policy selection matters** for workloads with stable hot sets. A regression here means the policy weighting has changed.

**Pass criteria:** `LFU_hit_rate ≥ 0.55` AND `LFU_hit_rate > LRU_hit_rate`.

---

### `minio_data_loader` — requires Docker

Uploads 10 Parquet files (512 KB each) to MinIO, then exercises the `S3DataLoader → LRUCache` path.

**What it checks:**
- All 10 files load without error from `s3://iceberg-stress/stress/parquet/`.
- Second-pass reads (identical keys) all return cache hits (100% hit rate).
- Cache-hit latency is lower than S3-load latency.

**Pass criteria:** zero load errors, 100% hit rate on second pass, cache faster than S3.

**Typical numbers on M4 + local MinIO:**
- S3 miss p50: ~3 ms (loopback network to Docker container)
- Cache hit p50: < 0.01 ms
- Speedup: ~6 000×

---

## Usage reference

```
./run_stress_test.sh [options]

Options:
  --skip-docker        Skip minio_data_loader; no Docker required
  --no-docker-mgmt     MinIO must already be running; script won't touch docker compose
  --keep-docker        Leave MinIO running after the suite finishes
  --only NAME          Run a single named scenario
  --list               Print all scenario names and exit
  --cache-mb N         Override default 64 MB cache budget
  --verbose            Print INFO-level logs from cache internals
  --no-color           Plain text output (useful in CI)
```

**Environment variables** (alternative to flags):

| Variable | Default | Effect |
|---|---|---|
| `STRESS_CACHE_MB` | `64` | Cache memory budget in MB |
| `MINIO_HOST` | `localhost` | MinIO hostname |
| `MINIO_PORT` | `9000` | MinIO API port |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO secret key |

## MinIO console

When MinIO is running you can inspect uploaded objects at:
- API: `http://localhost:9000`
- Console UI: `http://localhost:9001` (login: `minioadmin` / `minioadmin`)

## Adding a new scenario

1. Create `stress_tests/scenarios/my_scenario.py` — subclass `BaseScenario`, implement `run() -> ScenarioResult`.
2. Set `requires_docker = True` if the scenario needs MinIO.
3. Register it in `stress_tests/scenarios/__init__.py`.
4. Add an instance to `_SCENARIOS` in `stress_tests/runner.py`.

```python
# stress_tests/scenarios/my_scenario.py
from stress_tests.scenarios.base import BaseScenario
from stress_tests.metrics import ScenarioResult, Timer

class MyScenario(BaseScenario):
    name = "my_scenario"
    requires_docker = False

    def run(self) -> ScenarioResult:
        with Timer() as t:
            # exercise whatever you just built
            ...
        return ScenarioResult(
            name=self.name,
            passed=True,
            duration_s=t.elapsed_s,
            # ... fill remaining fields
        )
```

## File layout

```
stress_tests/
  __init__.py          # sys.path bootstrap (makes src/ importable)
  config.py            # MinIO + cache config; all values env-overridable
  metrics.py           # ScenarioResult, LatencyTracker, Timer
  data_factory.py      # Arrow table generator, MinIO Parquet uploader
  scenarios/
    base.py            # BaseScenario ABC
    cache_throughput.py
    memory_pressure.py
    concurrent_access.py
    eviction_benchmark.py
    minio_data_loader.py
  runner.py            # CLI entry point
docker/
  docker-compose.yml   # MinIO service + bucket-init one-shot
run_stress_test.sh     # Executable wrapper (handles Docker lifecycle)
```
