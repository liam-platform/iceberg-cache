#!/usr/bin/env python3
"""Seed local Iceberg tables with realistic sample data for local development.

Creates two tables in MinIO via PyIceberg SqlCatalog (SQLite-backed):
  - default.orders  — 100 000 rows of e-commerce orders
  - default.events  — 300 000 rows of user clickstream events

Run from the project root:
    python scripts/seed_data.py [--rows-orders N] [--rows-events N] [--reset]
"""

import argparse
import pathlib
import sys
import time
from datetime import date, timedelta

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Change to project root so sqlite:///local-data/catalog.db resolves correctly
import os
os.chdir(PROJECT_ROOT)

import numpy as np
import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

CONFIG_PATH = PROJECT_ROOT / "config" / "local.yaml"
LOCAL_DATA = PROJECT_ROOT / "local-data"


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def _make_orders(n_rows: int, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed)

    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    regions = ["north", "south", "east", "west"]
    base = date(2024, 1, 1)

    order_ids    = np.arange(1, n_rows + 1, dtype=np.int64)
    customer_ids = rng.integers(1, 10_001, size=n_rows, dtype=np.int64)
    product_ids  = rng.integers(1, 1_001,  size=n_rows, dtype=np.int64)
    quantities   = rng.integers(1, 21,     size=n_rows).astype(np.int32)
    unit_prices  = np.round(rng.uniform(5.0, 500.0, size=n_rows), 2)
    total_amounts = np.round(quantities.astype(np.float64) * unit_prices, 2)
    status_col   = [statuses[i] for i in rng.integers(0, len(statuses), size=n_rows)]
    region_col   = [regions[i]  for i in rng.integers(0, len(regions),  size=n_rows)]
    order_dates  = [base + timedelta(days=int(d)) for d in rng.integers(0, 365, size=n_rows)]

    return pa.table({
        "order_id":     pa.array(order_ids,    type=pa.int64()),
        "customer_id":  pa.array(customer_ids, type=pa.int64()),
        "product_id":   pa.array(product_ids,  type=pa.int64()),
        "quantity":     pa.array(quantities,   type=pa.int32()),
        "unit_price":   pa.array(unit_prices,  type=pa.float64()),
        "total_amount": pa.array(total_amounts, type=pa.float64()),
        "status":       pa.array(status_col,   type=pa.string()),
        "region":       pa.array(region_col,   type=pa.string()),
        "order_date":   pa.array(order_dates,  type=pa.date32()),
    })


def _make_events(n_rows: int, seed: int = 99) -> pa.Table:
    rng = np.random.default_rng(seed)

    event_types = ["click", "view", "purchase", "search", "logout"]
    pages = ["/home", "/product", "/cart", "/checkout", "/search", "/account"]
    base = date(2024, 1, 1)

    event_ids    = np.arange(1, n_rows + 1, dtype=np.int64)
    user_ids     = rng.integers(1, 50_001, size=n_rows, dtype=np.int64)
    event_col    = [event_types[i] for i in rng.integers(0, len(event_types), size=n_rows)]
    page_col     = [pages[i]       for i in rng.integers(0, len(pages),       size=n_rows)]
    session_ids  = [f"sess_{i:08d}" for i in rng.integers(0, 1_000_000, size=n_rows)]
    duration_ms  = rng.integers(50, 30_001, size=n_rows).astype(np.int32)
    event_dates  = [base + timedelta(days=int(d)) for d in rng.integers(0, 365, size=n_rows)]

    return pa.table({
        "event_id":    pa.array(event_ids,   type=pa.int64()),
        "user_id":     pa.array(user_ids,    type=pa.int64()),
        "event_type":  pa.array(event_col,   type=pa.string()),
        "page":        pa.array(page_col,    type=pa.string()),
        "session_id":  pa.array(session_ids, type=pa.string()),
        "duration_ms": pa.array(duration_ms, type=pa.int32()),
        "event_date":  pa.array(event_dates, type=pa.date32()),
    })


# ---------------------------------------------------------------------------
# Iceberg schema definitions
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = Schema(
    NestedField(1, "order_id",     LongType(),    required=True),
    NestedField(2, "customer_id",  LongType()),
    NestedField(3, "product_id",   LongType()),
    NestedField(4, "quantity",     IntegerType()),
    NestedField(5, "unit_price",   DoubleType()),
    NestedField(6, "total_amount", DoubleType()),
    NestedField(7, "status",       StringType()),
    NestedField(8, "region",       StringType()),
    NestedField(9, "order_date",   DateType()),
)

EVENTS_SCHEMA = Schema(
    NestedField(1, "event_id",    LongType(),    required=True),
    NestedField(2, "user_id",     LongType()),
    NestedField(3, "event_type",  StringType()),
    NestedField(4, "page",        StringType()),
    NestedField(5, "session_id",  StringType()),
    NestedField(6, "duration_ms", IntegerType()),
    NestedField(7, "event_date",  DateType()),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config():
    with open(CONFIG_PATH) as fh:
        return yaml.safe_load(fh)


def _verify_minio(aws_cfg: dict) -> None:
    import boto3
    from botocore.exceptions import EndpointResolutionError, NoCredentialsError

    try:
        client = boto3.client("s3", **aws_cfg)
        client.list_buckets()
    except Exception as exc:
        print(
            f"\n  ERROR: Cannot reach MinIO at {aws_cfg.get('endpoint_url')}.\n"
            "  Make sure the stack is running:\n\n"
            "      docker compose -f docker/docker-compose.yml up -d\n\n"
            f"  Underlying error: {exc}\n"
        )
        sys.exit(1)


def _seed_table(
    catalog,
    table_identifier: str,
    schema: Schema,
    arrow_table: pa.Table,
    location: str,
    reset: bool,
) -> None:
    print(f"  Seeding {table_identifier} ({len(arrow_table):,} rows)...", end="", flush=True)
    t0 = time.perf_counter()

    if reset:
        try:
            catalog.drop_table(table_identifier)
        except NoSuchTableError:
            pass

    if catalog.table_exists(table_identifier):
        tbl = catalog.load_table(table_identifier)
        tbl.overwrite(arrow_table)
    else:
        tbl = catalog.create_table(table_identifier, schema=schema, location=location)
        tbl.append(arrow_table)

    elapsed = time.perf_counter() - t0
    size_mb = arrow_table.nbytes / 1024 / 1024
    print(f" done ({elapsed:.1f}s, {size_mb:.0f} MB in-memory)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Seed local Iceberg tables")
    parser.add_argument("--rows-orders", type=int, default=100_000, metavar="N",
                        help="Number of rows for default.orders (default: 100000)")
    parser.add_argument("--rows-events", type=int, default=300_000, metavar="N",
                        help="Number of rows for default.events (default: 300000)")
    parser.add_argument("--reset", action="store_true",
                        help="Drop and re-create tables if they already exist")
    args = parser.parse_args()

    print("\niceberg-cache local data seed")
    print("=" * 40)

    config = _load_config()
    aws_cfg = config["aws"]
    catalog_cfg = config["iceberg_catalog"]

    # Verify MinIO is reachable before writing anything
    print("Checking MinIO connectivity...", end=" ", flush=True)
    _verify_minio(aws_cfg)
    print("OK")

    # Ensure local-data/ directory exists for SQLite catalog
    LOCAL_DATA.mkdir(exist_ok=True)

    print("Connecting to Iceberg SqlCatalog (SQLite)...", end=" ", flush=True)
    catalog = load_catalog("default", **catalog_cfg)
    print("OK")

    # Create namespace
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    print("\nGenerating and writing tables:")

    _seed_table(
        catalog,
        "default.orders",
        ORDERS_SCHEMA,
        _make_orders(args.rows_orders),
        "s3://warehouse/default/orders",
        args.reset,
    )

    _seed_table(
        catalog,
        "default.events",
        EVENTS_SCHEMA,
        _make_events(args.rows_events),
        "s3://warehouse/default/events",
        args.reset,
    )

    print("\nAll tables seeded. Query them via the Flight server:")
    print("  default.orders")
    print("  default.events")
    print()


if __name__ == "__main__":
    main()
