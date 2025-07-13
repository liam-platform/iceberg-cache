# Example client code
import pyarrow.flight as flight
import pyarrow as pa


class CacheClient:
    """Simple Arrow Flight client for the cache"""
    
    def __init__(self, server_location: str = "grpc://localhost:8815"):
        self.client = flight.FlightClient(server_location)
    
    def query_table(self, table_id: str) -> pa.Table:
        """Query a table"""
        flight_info = self.client.get_flight_info(
            flight.FlightDescriptor.for_path(table_id)
        )
        
        reader = self.client.do_get(flight_info.endpoints[0].ticket)
        return reader.read_all()
    
    def query_sql(self, sql: str) -> pa.Table:
        """Execute SQL query (limited support in MVP)"""
        # This would need to be implemented with a proper SQL endpoint
        pass
