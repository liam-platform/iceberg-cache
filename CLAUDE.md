# CLAUDE.md

## Project Overview

`iceberg-cache` (package name: `liam-cache`) is an in-memory cache layer for Apache Iceberg tables stored on S3. It loads Iceberg partitions into Apache Arrow columnar format once, then serves subsequent reads from memory over Arrow Flight gRPC — eliminating repeated metadata resolution, S3 round-trips, and Parquet deserialization.

## Tech Stack

- **Python 3.13+**
- **PyArrow / Arrow Flight** — columnar in-memory format and gRPC transport (port 8815)
- **PyIceberg** — Iceberg catalog integration, metadata, snapshots
- **DataFusion ≥48** — in-process SQL query engine
- **boto3** — S3 Parquet loading
- **psutil** — system memory introspection (80% RAM ceiling)

## Directory Structure

```
src/
  core/               # Cache internals
    cache_node.py         # ArrowCacheNode — main orchestrator
    lru_cache.py          # OrderedDict-based LRU with size tracking
    eviction_policy.py    # Pluggable eviction: LRU, LFU, CustomEvictionPolicy
    arrow_memory_management.py  # Allocation tracking + eviction under memory pressure
    bloom_filter.py       # Predicate pruning on string columns
    cache_data_model.py   # CacheKey, CacheEntry, PartitionInfo, CacheLocation
    config.py             # CacheConfig placeholder
  flight_server/
    server.py             # ArrowFlightServer — gRPC endpoint
  iceberg_management/
    metadata.py           # IcebergMetadataManager — catalog, schema, partition discovery, time-travel
  storage/
    dataloader.py         # S3DataLoader — sync + async Parquet loading via ThreadPoolExecutor
  sql/
    engine.py             # QueryEngine — DataFusion SessionContext wrapper
  coordinator/            # Distributed coordination (WIP, not active)
  examples/               # Example Flight clients
  tests/                  # pytest test suite
docs/
  arrow_iceberg_cache_design.md   # Full design spec
pyproject.toml            # Dependencies, test config (pythonpath: src, asyncio_mode: auto)
conftest.py               # sys.path setup for bare imports
```

## Component Interaction

```
Client (Arrow Flight gRPC :8815)
  └─→ ArrowFlightServer  (flight_server/server.py)
        └─→ ArrowCacheNode  (core/cache_node.py)  ← central orchestrator
              ├─→ IcebergMetadataManager  →  PyIceberg catalog (schema, partitions, snapshots)
              ├─→ S3DataLoader            →  boto3 (reads Parquet from S3)
              ├─→ LRUCache               ←  unified partition store
              │     └─→ ArrowMemoryManager  (enforces 80% RAM ceiling)
              ├─→ BloomFilter + min/max stats  (built per entry, predicate pruning)
              └─→ Background daemon thread  (TTL eviction every 60s)

QueryEngine  (sql/engine.py)
  └─→ DataFusion SessionContext  ←  registers cached Arrow tables as views
```

## Key Design Details

- **Cache key format:** `table_id#partition_spec#columns_hash` (partition spec JSON-serialized with sorted keys for determinism)
- **Memory eviction:** `_ensure_memory_available()` evicts before each load; CustomEvictionPolicy weights age 40% + frequency inverse 40% + size 20%
- **Thread safety:** `RLock` on `ArrowCacheNode`; `Lock` on `LRUCache` and `ArrowMemoryManager`
- **Time-travel:** `IcebergMetadataManager` supports point-in-time reads via timestamp or snapshot ID
- **Flight streaming:** batches of 10k rows per `RecordBatch`
- **Partition merge:** `get_all_tables()` concatenates partitions by `table_id` for DataFusion registration

## Running Tests

```bash
pytest src/tests/
```
