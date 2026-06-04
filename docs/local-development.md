# Local Development Guide

This guide walks you through running iceberg-cache end-to-end on your laptop using MinIO as a local S3 backend and a SQLite-backed Iceberg catalog.

## Architecture (local stack)

```
┌────────────────────────────────────────────┐
│  Docker                                    │
│  MinIO :9000  ←  Parquet files (warehouse) │
└────────────────────────────────────────────┘
         ↑ S3 API (boto3 + PyIceberg)
scripts/seed_data.py   ← run once to populate
         ↓
local-data/catalog.db  ← SQLite Iceberg catalog
         ↓
src/main.py            ← Arrow Flight server :8815
         ↓ gRPC
src/examples/client/demo.py  ← performance demo
```

---

## Prerequisites

| Tool | Install |
|---|---|
| **Docker Desktop** (or colima) | https://www.docker.com/products/docker-desktop |
| **Python 3.13+** | https://www.python.org |
| **uv** (package manager) | `curl -Lsf https://astral.sh/uv/install.sh \| sh` |

---

## Quickstart

All commands are run from the **project root**.

### 1. Install dependencies

```bash
uv sync
```

### 2. Start MinIO

```bash
docker compose -f docker/docker-compose.yml up -d
```

MinIO console is available at http://localhost:9001 (user: `minioadmin`, password: `minioadmin`).

Verify it's healthy:
```bash
docker compose -f docker/docker-compose.yml ps
```

### 3. Seed the Iceberg tables

Creates `default.orders` (100 k rows) and `default.events` (300 k rows) in MinIO.

```bash
uv run python scripts/seed_data.py
```

Expected output:
```
iceberg-cache local data seed
========================================
Checking MinIO connectivity... OK
Connecting to Iceberg SqlCatalog (SQLite)... OK

Generating and writing tables:
  Seeding default.orders (100,000 rows)... done (3.2s, 14 MB in-memory)
  Seeding default.events (300,000 rows)... done (8.1s, 30 MB in-memory)

All tables seeded. Query them via the Flight server:
  default.orders
  default.events
```

> Seeding only needs to run once. Re-run with `--reset` to wipe and recreate the tables.

### 4. Start the Arrow Flight server

```bash
uv run python src/main.py
```

Expected output:
```
Starting iceberg-cache  (cache=512 MB, policy=lru)
Connecting to Iceberg catalog...
Arrow Flight server listening on 0.0.0.0:8815
Press Ctrl+C to stop.
```

### 5. Run the performance demo

In a **second terminal**:

```bash
uv run python src/examples/client/demo.py
```

Expected output (numbers vary by machine):
```
iceberg-cache demo
Connecting to grpc://localhost:8815... OK  (2 tables available)

Available tables: default.orders, default.events
Warm reads per table: 10
────────────────────────────────────────────────────────

  Table: default.orders
    cold read              1 823.4 ms  ████████████████████████████████████
    cache hit p50              2.1 ms  ▌
    cache hit p95              3.0 ms  ▌
    speedup: 868×

  Table: default.events
    cold read              4 291.7 ms  ████████████████████████████████████████
    cache hit p50              5.8 ms  ██
    cache hit p95              7.2 ms  ██
    speedup: 740×

────────────────────────────────────────────────────────
Tip: cold read = S3 round-trip + Parquet decode.
      cache hit = Arrow in-memory copy, no I/O.
```

---

## Exploring data manually

Install a Flight client (the project ships one):

```python
# From any Python REPL with the venv active
import sys; sys.path.insert(0, "src"); sys.path.insert(0, "src/examples/client")
from flight_client import CacheClient

client = CacheClient()
print(client.list_tables())           # ['default.orders', 'default.events']

orders = client.query_table("default.orders")
print(orders.schema)
print(orders.slice(0, 5).to_pandas())
```

---

## Running the stress tests

With MinIO up and data seeded:

```bash
uv run python stress_tests/runner.py            # all scenarios (correctness + stress)
uv run python stress_tests/runner.py --only minio_data_loader   # single scenario
uv run python stress_tests/runner.py --skip-docker              # offline-only scenarios
```

---

## Development workflow

### After changing cache internals (`src/core/`, `src/storage/`, etc.)

```bash
# 1. Run the unit + integration tests
uv run pytest src/tests/

# 2. Restart the server to pick up changes
#    (Ctrl+C the running server, then)
uv run python src/main.py

# 3. Re-run the demo to verify end-to-end behaviour
uv run python src/examples/client/demo.py
```

The server must be **restarted** for code changes to take effect — it loads all modules at startup and keeps state in memory for its lifetime.

### After changing IcebergMetadataManager or catalog config

If you change how the catalog is accessed or modify `config/local.yaml`:

```bash
# Restart the server (picks up new config + code)
uv run python src/main.py
```

The SQLite catalog at `local-data/catalog.db` and the data in MinIO are not affected by server restarts. Only the **in-memory cache** is lost.

### After a schema change to sample tables

If you change `scripts/seed_data.py` (e.g., add columns, change table shape):

```bash
# Wipe and re-create both tables
uv run python scripts/seed_data.py --reset

# Then restart the server
uv run python src/main.py
```

### Adding a new feature end-to-end checklist

1. Write / update tests in `src/tests/`
2. Implement in `src/`
3. `uv run pytest src/tests/` — all tests pass
4. Restart server: `uv run python src/main.py`
5. Run demo: `uv run python src/examples/client/demo.py`
6. Run stress suite: `uv run python stress_tests/runner.py`

---

## Configuration reference (`config/local.yaml`)

| Key | Default | Notes |
|---|---|---|
| `iceberg_catalog.uri` | `sqlite:///local-data/catalog.db` | SQLite DB path (relative to project root) |
| `iceberg_catalog.warehouse` | `s3://warehouse/` | MinIO bucket for Iceberg data files |
| `iceberg_catalog.s3.endpoint` | `localhost:9000` | MinIO endpoint (no scheme) |
| `iceberg_catalog.s3.ssl-enabled` | `"false"` | Must be `"false"` for HTTP MinIO |
| `aws.endpoint_url` | `http://localhost:9000` | boto3 endpoint (with scheme) |
| `cache.max_memory_mb` | `512` | Arrow cache ceiling in MB |
| `cache.policy` | `lru` | `lru`, `lfu`, or `custom` |
| `server.port` | `8815` | Arrow Flight gRPC port |

---

## Troubleshooting

### `Cannot reach MinIO` (seed script fails at connectivity check)

```bash
# Check if MinIO container is running
docker compose -f docker/docker-compose.yml ps

# Start it if it isn't
docker compose -f docker/docker-compose.yml up -d

# Tail MinIO logs
docker compose -f docker/docker-compose.yml logs minio
```

### `SSL: WRONG_VERSION_NUMBER` or similar TLS errors during seeding

Ensure `s3.ssl-enabled: "false"` is present in `config/local.yaml`. This tells PyIceberg's PyArrow file I/O to use HTTP instead of HTTPS when writing to MinIO.

### `No tables found` in demo

Either the seed script hasn't been run yet, or it used a different catalog DB. Check:

```bash
ls -lh local-data/catalog.db        # should exist after seeding
uv run python scripts/seed_data.py  # re-run if missing
```

### Server starts but `do_get` returns empty table

The catalog might reference stale snapshot data. Re-seed with `--reset`:

```bash
uv run python scripts/seed_data.py --reset
uv run python src/main.py  # restart
```

### Port 8815 already in use

```bash
lsof -i :8815        # find what's using it
kill <PID>           # stop it
```

Or change the port in `config/local.yaml` (`server.port: 8816`) and pass `--port 8816` to the demo.

### Resetting everything

```bash
# Stop Docker stack and remove volumes
docker compose -f docker/docker-compose.yml down -v

# Remove local catalog DB
rm -rf local-data/

# Start fresh
docker compose -f docker/docker-compose.yml up -d
uv run python scripts/seed_data.py
uv run python src/main.py
```
