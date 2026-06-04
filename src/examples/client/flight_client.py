"""Arrow Flight client for iceberg-cache."""

import time
from typing import Iterator, Optional

import pyarrow as pa
import pyarrow.flight as flight


class CacheClient:
    """Arrow Flight client for iceberg-cache."""

    def __init__(self, server_location: str = "grpc://localhost:8815"):
        self.client = flight.FlightClient(server_location)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Return the names of all tables visible in the cache."""
        return [
            info.descriptor.path[0].decode()
            for info in self.client.list_flights()
        ]

    def query_table(
        self,
        table_id: str,
        *,
        columns: Optional[list[str]] = None,
    ) -> pa.Table:
        """Fetch a full table from the cache.

        Args:
            table_id: Dot-separated table name, e.g. ``"default.orders"``.
            columns:  If given, only these columns are requested (projection
                      is advisory — the server may return all columns).
        """
        descriptor = flight.FlightDescriptor.for_path(table_id.encode())
        info = self.client.get_flight_info(descriptor)
        reader = self.client.do_get(info.endpoints[0].ticket)
        table = reader.read_all()
        if columns:
            available = set(table.column_names)
            requested = [c for c in columns if c in available]
            table = table.select(requested)
        return table

    def timed_query(self, table_id: str) -> tuple[pa.Table, float]:
        """Run query_table and return (table, elapsed_ms)."""
        t0 = time.perf_counter()
        table = self.query_table(table_id)
        return table, (time.perf_counter() - t0) * 1000

    def get_cache_stats(self) -> dict:
        """Retrieve cache statistics via a dedicated action (best-effort)."""
        try:
            results = list(self.client.do_action(flight.Action("cache_stats", b"")))
            if results:
                import json
                return json.loads(results[0].body.to_pybytes())
        except Exception:
            pass
        return {}
