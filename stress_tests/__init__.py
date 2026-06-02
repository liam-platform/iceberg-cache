"""Stress test suite for iceberg-cache.

Standalone scenarios require no external dependencies.
Integration scenarios (tag: requires_docker) need MinIO running via docker/docker-compose.yml.
"""
import pathlib
import sys

# Make src/ importable from anywhere inside this package
_SRC = pathlib.Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
