#!/usr/bin/env python3
"""iceberg-cache Arrow Flight server entrypoint.

Usage (from project root):
    python src/main.py [--config config/local.yaml] [--verbose]
"""

import argparse
import logging
import os
import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Change to project root so sqlite:///local-data/catalog.db resolves correctly
os.chdir(PROJECT_ROOT)

import yaml

from core.cache_data_model import CachePolicy
from core.cache_node import ArrowCacheNode
from flight_server.server import ArrowFlightServer

_POLICIES = {"lru": CachePolicy.LRU, "lfu": CachePolicy.LFU, "custom": CachePolicy.CUSTOM}


def _load_config(path: pathlib.Path) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def main() -> None:
    parser = argparse.ArgumentParser(description="iceberg-cache Flight server")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config" / "local.yaml"),
        help="Path to YAML config (default: config/local.yaml)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable INFO-level logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    config_path = pathlib.Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        print("Run from the project root and ensure config/local.yaml exists.", file=sys.stderr)
        sys.exit(1)

    config = _load_config(config_path)

    cache_cfg = config.get("cache", {})
    max_memory_bytes = cache_cfg.get("max_memory_mb", 512) * 1024 * 1024
    policy = _POLICIES.get(cache_cfg.get("policy", "lru"), CachePolicy.LRU)

    server_cfg = config.get("server", {})
    host = server_cfg.get("host", "0.0.0.0")
    port = server_cfg.get("port", 8815)

    print(f"Starting iceberg-cache  (cache={cache_cfg.get('max_memory_mb', 512)} MB, policy={policy.value})")
    print(f"Connecting to Iceberg catalog...")

    cache_node = ArrowCacheNode(
        config=config,
        max_memory_bytes=max_memory_bytes,
        cache_policy=policy,
    )

    location = f"grpc://{host}:{port}"
    server = ArrowFlightServer(cache_node, location=location)

    print(f"Arrow Flight server listening on {host}:{port}")
    print("Press Ctrl+C to stop.\n")

    try:
        server.serve()
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()
