import threading
import time
from unittest import mock

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from flight_server.server import ArrowFlightServer  # Adjust import path


@pytest.fixture(scope="module")
def dummy_table():
    return pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

@pytest.fixture(scope="module")
def mock_cache_node(dummy_table):
    mock_node = mock.MagicMock()
    mock_node.get_table_data.return_value = dummy_table
    mock_node.metadata_manager.get_schema.return_value = dummy_table.schema
    return mock_node

@pytest.fixture(scope="module")
def flight_server(mock_cache_node):
    server = ArrowFlightServer(cache_node=mock_cache_node, location="grpc://0.0.0.0:8815")
    
    # Run the server in a background thread
    server_thread = threading.Thread(target=server.serve)
    server_thread.daemon = True
    server_thread.start()

    # Wait briefly to ensure the server is up
    time.sleep(1)
    
    yield server

    server.shutdown()
    server_thread.join()

def test_get_flight_info(flight_server):
    client = flight.FlightClient("grpc://localhost:8815")
    table_id = "test_table"
    descriptor = flight.FlightDescriptor.for_path(table_id)

    info = client.get_flight_info(descriptor)

    assert isinstance(info, flight.FlightInfo)
    assert info.descriptor.path[0].decode() == table_id
    assert info.schema.equals(flight_server.cache_node.metadata_manager.get_schema(table_id))

def test_do_get(flight_server, dummy_table):
    client = flight.FlightClient("grpc://localhost:8815")
    ticket = flight.Ticket("test_table".encode())

    reader = client.do_get(ticket)
    received_table = reader.read_all()

    assert received_table.equals(dummy_table)
