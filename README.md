# iceberg-cache

## Problem

Querying Apache Iceberg tables on S3 is expensive. Every query requires round-trips to resolve Iceberg metadata, then fetch and deserialize Parquet files from object storage. Repeated access to the same tables or partitions pays this cost each time.

## Solution

An in-memory cache layer that loads Iceberg table partitions from S3 into Apache Arrow columnar format once, then serves subsequent reads from memory over the Arrow Flight gRPC protocol — zero re-serialization, zero object-store round-trips on cache hits.

## Architecture

```
Client
  │  Arrow Flight (gRPC)
  ▼
ArrowFlightServer          ← exposes cache over Flight protocol
  │
ArrowCacheNode             ← core: unified LRU store, eviction, index structures
  ├── IcebergMetadataManager  ← resolves schema + partition file paths from catalog
  ├── S3DataLoader            ← reads Parquet files into Arrow Tables
  ├── QueryEngine             ← DataFusion SQL over registered Arrow views
  └── DistributedCacheCoordinator (interface)  ← multi-node coordination (WIP)
```

### Cache node internals

**Storage**: a single size-bounded `LRUCache` keyed by `table_id#partition_spec#columns`. Cache keys are deterministic so the same logical query always hits the same entry.

**Eviction**: pluggable policies — LRU (recency), LFU (frequency), or a composite scorer weighting age (40%), inverse frequency (40%), and entry size (20%). A background daemon thread additionally enforces a 1-hour TTL.

**Index structures**: on every cache population, per-entry Bloom filters (string columns) and min/max statistics (numeric/date columns) are built to support future predicate pruning.

**Memory management**: `ArrowMemoryManager` tracks allocated bytes against an 80%-of-system-RAM ceiling. On cache miss, memory pressure triggers eviction before loading new data.

**Query layer**: `QueryEngine` wraps DataFusion — cached Arrow Tables are registered as views, then arbitrary SQL runs directly in-process against columnar memory.

**Transport**: `ArrowFlightServer` wraps a cache node and exposes `list_flights` / `get_flight_info` / `do_get` over Arrow Flight gRPC on port 8815. Data is streamed as Arrow record batches (10k rows/batch), avoiding any intermediate serialization.

## Stress test
Refer to `stress_tests` module for more details of how stress test can be configured and executed. A quick glance on the performance of all the stress test scenarios running on a 16GB Mac Mini M4 can be found below:

| Scenario | Typical throughput | p99 latency |
|---|---|---|
| `cache_throughput` | ~1.7 M ops/s | < 1 ms |
| `memory_pressure` | ~1.3 M ops/s | < 1 ms |
| `concurrent_access` | ~300 K ops/s | < 1 ms |
| `eviction_benchmark` | ~1.5 M ops/s | — |
| `minio_data_loader` | ~500 ops/s (S3-bound) | — |

## Stack

- [PyIceberg](https://py.iceberg.apache.org/) — catalog and metadata resolution
- [Apache Arrow](https://arrow.apache.org/docs/python/) — columnar in-memory format
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — gRPC transport
- [DataFusion](https://datafusion.apache.org/) — SQL query engine
- AWS S3 — backing object store (Parquet files)
