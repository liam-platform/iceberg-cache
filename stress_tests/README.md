# Stress Tests

A repeatable stress test suite for `iceberg-cache` — run it after every new feature to catch regressions and find breaking points.

The suite has two categories with different semantics:

| Category | Semantics |
|---|---|
| **Correctness** | Cache invariants must hold. Any failure is a bug. |
| **Stress** | Designed to find breaking points. The question is not "did it break?" but "how did it break and can it recover?" Graceful failure, quantified degradation, and clean recovery are *passing* outcomes. |

## Requirements

| Dependency | Version | Notes |
|---|---|---|
| Python | ≥ 3.13 | Match the project's minimum |
| Docker Desktop | latest (Apple Silicon) | Only needed for MinIO / integration tests |
| uv | any | `uv sync` to install project deps |

> **No extra packages needed.** The stress tests use only dependencies already declared in `pyproject.toml` (`pyarrow`, `boto3`, `psutil`).

## Quick start

```bash
# Standalone scenarios only — no Docker, runs in ~30 s
./run_stress_test.sh --skip-docker

# Full suite — starts MinIO automatically
./run_stress_test.sh

# Single scenario
./run_stress_test.sh --only spike_test

# List all scenario names
./run_stress_test.sh --list
```

## Mac M4 (16 GB unified memory) — notes

The M4 chip's unified memory architecture gives the CPU and Arrow memory pool access to the same high-bandwidth pool (~100 GB/s). A few practical implications:

**Default cache budget is 64 MB.** This is intentionally small — it exercises eviction pressure without inflating RSS. On 16 GB you can raise it to stress-test at production scale:

```bash
./run_stress_test.sh --cache-mb 2048   # 2 GB cache
```

**Reading the output.** Each run prints live measurements — throughput, latency percentiles, hit rate, and eviction count. Use those numbers as your baseline. In-memory scenarios (no I/O) typically run in the millions of ops/second on M4; the MinIO scenario is network-bound and will be orders of magnitude slower. A significant drop in ops/s or a jump in p99 after a code change is the signal to investigate.

```
  CORRECTNESS  —  invariants must hold
  ────────────────────────────────────────────────────────────────────

  [PASS]  cache_throughput    0.01s  1,347,870 ops/s  p50=<1µs  p99=<1µs  hit=55%  evict=22
  [PASS]  minio_data_loader   0.04s        507 ops/s  p50=<1µs  p99=<1µs  hit=100%  evict=0

  STRESS  —  find the breaking point
  ────────────────────────────────────────────────────────────────────

  [PASS]  spike_test          9.03s   319,021 ops/s  ...
  baseline=319,021  spike=140,808 (44% of baseline)  recovery=314,890 (99%)
```

**Docker Desktop on Apple Silicon.** Use Docker Desktop ≥ 4.30 with the `Use Rosetta for x86_64/amd64 emulation` setting disabled — native ARM images are used for both `minio/minio` and `minio/mc`.

## Correctness scenarios

### `cache_throughput`

Loads 50 Arrow tables (512 KB each) into a 16 MB cache, then performs 10 000 random reads.

**What it checks:**
- Eviction fires before the 28th insert (budget = 32 × 512 KB = 16 MB)
- `current_size_bytes` never exceeds the budget after every `put()`
- p99 `get()` latency stays below 10 ms
- Hit rate > 40% (cache holds ~28 of 50 tables at steady state)

**Pass criteria:** all four conditions above are met.

---

### `memory_pressure`

Loads 60 tables × 1 MB into a 20 MB budget (3× overload).

**What it checks:**
- `current_size_bytes ≤ max_size_bytes` is an invariant that must hold after **every single** `put()` call — not just at the end.
- Data returned by `get()` is never empty or schema-corrupted.
- Eviction fires for every table that does not fit.

**Pass criteria:** zero budget violations, zero data corruption across 3 000 reads.

---

### `concurrent_access`

Spawns 8 threads. Each thread performs 1 000 operations: 75% reads (`get`), 25% writes (`put`), targeting randomly chosen keys, against a single shared `LRUCache`.

**What it checks:**
- All 8 threads finish within a 30-second wall-clock timeout (no deadlocks).
- `current_size_bytes ≤ max_size_bytes` at the end (no double-counting under concurrent writes).
- Zero unhandled exceptions (`MemoryError` from `put()` under pressure is caught and not counted as an error).

**Pass criteria:** all threads complete, within budget, zero unhandled exceptions.

---

### `eviction_benchmark`

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

Typical numbers on M4 + local MinIO: S3 miss ~3 ms, cache hit <0.01 ms, speedup ~6 000×.

---

## Stress scenarios

### `spike_test`

Simulates an 8× traffic surge then measures whether throughput recovers.

**Phases:**

| Phase | Threads | Write ratio | Duration |
|---|---|---|---|
| Baseline | 4 | 25% | 3 s |
| Spike | 32 | 70% | 3 s |
| Recovery | 4 | 25% | 3 s |

The spike is write-heavy to maximise eviction pressure. During it, throughput drops to ~44% of baseline due to lock contention and constant eviction churn. The critical measurement is the *recovery* ratio — the system must return to ≥ 70% of baseline throughput once load normalises.

**Breaking point probed:** does eviction thrash under a spike cause deadlock, corrupt size accounting, or prevent recovery?

**Pass criteria:** within budget throughout, zero unhandled exceptions, recovery ≥ 70% of baseline.

---

### `soak_20s`

Runs 6 threads for 20 seconds, sampling throughput every 4 seconds. The output includes a per-window timeline so degradation is visible even when below the failure threshold:

```
timeline(ops/s): [230K  233K  233K  233K  233K]  degradation=-1.3%
```

**Breaking points probed:**
- **Throughput cliff** — ops/s drops >30% from first to last window, indicating eviction overhead growing or lock contention worsening over time.
- **Memory drift** — `current_size_bytes` creeps beyond budget across windows, indicating a size-accounting leak.

**Pass criteria:** degradation < 30%, within budget at every sample, zero unhandled exceptions.

---

### `oversize_entry`

Inserts a 25 MB table into a 20 MB cache (budget exceeded by 25%).

**What happens (the breaking point):**

1. Pre-warm: 5 entries × 2 MB = 10 MB (cache is 50% full).
2. Insert 25 MB entry → LRU evicts all 5 pre-warmed entries attempting to free space.
3. Still not enough room → `MemoryError` raised.
4. Cache is now **empty** — all previously cached data is gone.

This is *catastrophic eviction*: the eviction mechanism destroys existing data without achieving its goal. It is not a bug — it is documented behaviour — but callers must handle `MemoryError` and be aware the cache will be empty afterwards.

**Pass criteria:** `MemoryError` is raised (not a crash), `current_size_bytes == 0` after the failure (not leaked or negative), and the cache accepts normal-sized inserts immediately after.

---

### `cache_thrash`

Loads 200 unique tables sequentially into a cache sized for 4 tables (50× over capacity). Every insert from the 5th onwards evicts an existing entry — the eviction path runs on 100% of operations.

This is the worst-case access pattern for an analytical workload: a sequential full-table scan with zero temporal locality.

**Breaking points probed:**
- Does `put()` latency blow up as eviction overhead accumulates?
- Does `current_size_bytes` stay correct after hundreds of evictions?
- Does the system crash, or degrade gracefully to a lower-but-stable throughput?

**Pass criteria:** zero budget violations, zero data corruption, `put()` p99 < 50 ms even at 98% eviction rate.

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
2. Set `category = "correctness"` or `category = "stress"`.
3. Set `requires_docker = True` if the scenario needs MinIO.
4. Register it in `stress_tests/scenarios/__init__.py`.
5. Add an instance to `_CORRECTNESS` or `_STRESS` in `stress_tests/runner.py`.

```python
# stress_tests/scenarios/my_scenario.py
from stress_tests.scenarios.base import BaseScenario
from stress_tests.metrics import ScenarioResult, Timer

class MyScenario(BaseScenario):
    name = "my_scenario"
    category = "stress"   # or "correctness"
    requires_docker = False

    def run(self) -> ScenarioResult:
        with Timer() as t:
            # push the system; record what breaks and how
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
  __init__.py              # sys.path bootstrap (makes src/ importable)
  config.py                # MinIO + cache config; all values env-overridable
  metrics.py               # ScenarioResult, LatencyTracker, Timer
  data_factory.py          # Arrow table generator, MinIO Parquet uploader
  scenarios/
    base.py                # BaseScenario ABC (name, category, requires_docker)
    cache_throughput.py    # correctness
    memory_pressure.py     # correctness
    concurrent_access.py   # correctness
    eviction_benchmark.py  # correctness
    minio_data_loader.py   # correctness, requires Docker
    spike_test.py          # stress — traffic surge + recovery
    soak_test.py           # stress — sustained load, detect drift
    oversize_entry.py      # stress — catastrophic eviction on oversize insert
    cache_thrash.py        # stress — 100% eviction rate, graceful degradation
  runner.py                # CLI entry point
docker/
  docker-compose.yml       # MinIO service + bucket-init one-shot
run_stress_test.sh         # Executable wrapper (handles Docker lifecycle)
```
