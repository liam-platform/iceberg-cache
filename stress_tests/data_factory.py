"""Generate synthetic Arrow tables and seed MinIO with Parquet test data."""

import io
import logging
import random
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Pre-computed category values to avoid rebuilding per call
_CATEGORIES = [f"cat_{i:03d}" for i in range(200)]


def make_arrow_table(
    n_rows: int,
    n_int_cols: int = 3,
    n_float_cols: int = 2,
    n_str_cols: int = 2,
    seed: int = 42,
) -> pa.Table:
    """Return a synthetic Arrow table with the given shape."""
    rng = random.Random(seed)
    cols: Dict[str, pa.Array] = {}

    for i in range(n_int_cols):
        cols[f"int_{i}"] = pa.array(
            [rng.randint(0, 100_000) for _ in range(n_rows)], type=pa.int64()
        )
    for i in range(n_float_cols):
        cols[f"float_{i}"] = pa.array(
            [rng.uniform(0.0, 1_000.0) for _ in range(n_rows)], type=pa.float64()
        )
    for i in range(n_str_cols):
        cols[f"str_{i}"] = pa.array(
            [rng.choice(_CATEGORIES) for _ in range(n_rows)], type=pa.string()
        )

    return pa.table(cols)


def make_table_sized(target_bytes: int, seed: int = 42) -> pa.Table:
    """Return a table whose nbytes is approximately target_bytes.

    Uses an empirical ~72 bytes/row for the default column mix.
    """
    bytes_per_row = 8 * 3 + 8 * 2 + 8 * 2  # int64×3, float64×2, str~8×2
    n_rows = max(1, target_bytes // bytes_per_row)
    t = make_arrow_table(n_rows, seed=seed)
    return t


def upload_parquet(
    table: pa.Table,
    bucket: str,
    key: str,
    aws_config: dict,
) -> str:
    """Write an Arrow table as Parquet to MinIO. Returns the s3:// path."""
    import boto3  # local import — only needed for integration scenarios

    client = boto3.client("s3", **aws_config)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buf.read())
    s3_path = f"s3://{bucket}/{key}"
    logger.debug("Uploaded %d rows → %s", len(table), s3_path)
    return s3_path


def ensure_bucket(bucket: str, aws_config: dict) -> None:
    """Create the MinIO bucket if it does not exist."""
    import boto3

    client = boto3.client("s3", **aws_config)
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
        logger.info("Created bucket: %s", bucket)
