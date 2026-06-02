"""Connection config for stress tests — all values overridable via env vars."""

import os

MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
MINIO_PORT = int(os.getenv("MINIO_PORT", "9000"))
MINIO_ENDPOINT = f"http://{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

STRESS_TEST_BUCKET = "iceberg-stress"

AWS_CONFIG = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "region_name": "us-east-1",
}

# Cache memory for stress tests: smaller than production so pressure kicks in quickly.
DEFAULT_CACHE_MB = int(os.getenv("STRESS_CACHE_MB", "64"))
DEFAULT_CACHE_BYTES = DEFAULT_CACHE_MB * 1024 * 1024
