import pyarrow as pa
import pyarrow.flight as flight
from pyarrow.flight import Location

from core.cache_node import ArrowCacheNode


class ArrowFlightServer(flight.FlightServerBase):
    def __init__(self, cache_node: ArrowCacheNode, location: str = "grpc://0.0.0.0:8815"):
        super().__init__(location)
        self.cache_node = cache_node
        self._location = location
    
    def list_flights(self, context, criteria):
        """List available tables"""
        # TODO: implement this method
        pass
    
    def get_flight_info(self, context, descriptor):
        """Get flight info for a table"""
        # Basic implementation
        table_id = descriptor.path[0].decode()
        
        # Get schema from Iceberg
        schema = self.cache_node.metadata_manager.get_schema(table_id)
        
        endpoint = flight.FlightEndpoint(
            ticket=flight.Ticket(table_id.encode()),
            locations=[Location.for_grpc_tcp("localhost", 8815)]
        )
        
        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,  # Unknown
            total_bytes=-1     # Unknown
        )
    
    def do_get(self, context, ticket):
        """Get table data"""
        table_id = ticket.ticket.decode()
        
        # Get data from cache
        table = self.cache_node.get_table_data(table_id)
        
        # Return as record batch stream
        return flight.GeneratorStream(schema=table.schema, generator=self._table_generator(table))
    
    def _table_generator(self, table: pa.Table):
        """Generator for table batches"""
        # Split into batches for large tables
        batch_size = 10000
        for i in range(0, len(table), batch_size):
            batch = table.slice(i, batch_size).to_batches()[0] if table.slice(i, batch_size).num_rows > 0 else None
            if batch:
                yield batch
