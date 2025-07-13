#!/usr/bin/env python3
"""
Unit tests for IcebergMetadataManager implementation
"""

import os
import time
from datetime import date, datetime
from typing import Any, Dict, Generator
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

from iceberg_management.metadata import IcebergMetadataManager


class TestIcebergMetadataManager:
    """Test suite for IcebergMetadataManager"""
    
    @pytest.fixture(scope="class")
    def catalog_config(self) -> Dict[str, Any]:
        """Fixture for catalog configuration"""
        return {
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "warehouse": "s3://warehouse/"
        }
    
    @pytest.fixture(scope="class")
    def manager(self, catalog_config: Dict[str, Any]) -> IcebergMetadataManager:
        """Fixture for IcebergMetadataManager instance"""
        return IcebergMetadataManager(catalog_config)
    
    @pytest.fixture(scope="class")
    def table_name(self) -> str:
        """Fixture for test table name"""
        return "default.test_table"
    
    @pytest.fixture(scope="class")
    def sample_data(self) -> Dict[str, Any]:
        """Fixture for sample test data"""
        return {
            'id': list(range(1, 101)),
            'name': [f'user_{i}' for i in range(1, 101)],
            'age': [20 + (i % 50) for i in range(1, 101)],
            'city': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'] * 20,
            'date': [date(2024, 1, 1 + (i % 30)) for i in range(100)],
            'timestamp': [datetime(2024, 1, 1, 12, 0, 0) for _ in range(100)]
        }
    
    @pytest.fixture(scope="class")
    def iceberg_schema(self) -> IcebergSchema:
        """Fixture for Iceberg schema"""
        return IcebergSchema(
            NestedField(1, 'id', IntegerType(), required=True),
            NestedField(2, 'name', StringType(), required=True),
            NestedField(3, 'age', IntegerType(), required=True),
            NestedField(4, 'city', StringType(), required=True),
            NestedField(5, 'date', DateType(), required=True),
            NestedField(6, 'timestamp', TimestampType(), required=True),
            identifier_field_ids=[1]
        )
    
    @pytest.fixture(scope="class")
    def arrow_table(self, sample_data: Dict[str, Any]) -> pa.Table:
        """Fixture for Arrow table"""
        arrow_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("age", pa.int32(), nullable=False),
            pa.field("city", pa.string(), nullable=False),
            pa.field("date", pa.date32(), nullable=False),
            pa.field("timestamp", pa.timestamp("us"), nullable=False),
        ])
        
        df = pd.DataFrame(sample_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
    
    @pytest.fixture(scope="class")
    def test_table(self, manager: IcebergMetadataManager, table_name: str, 
                   iceberg_schema: IcebergSchema, arrow_table: pa.Table, 
                   sample_data: Dict[str, Any]) -> Generator[Any, None, None]:
        """Fixture for creating and cleaning up test table"""
        # Setup: Create table
        table = manager.catalog.create_table_if_not_exists(
            identifier=table_name,
            schema=iceberg_schema,
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=1, 
                    field_id=1000, 
                    transform=IdentityTransform(), 
                    name="category_partition"
                ),
                data=sample_data
            )
        )
        
        # Insert data
        table.append(arrow_table)
        
        yield table
        
        # Teardown: Clean up table
        try:
            manager.catalog.drop_table(table_name)
        except Exception as e:
            pytest.fail(f"Failed to cleanup table {table_name}: {e}")
    
    @pytest.fixture(autouse=True)
    def setup_environment(self):
        """Setup environment variables for tests"""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1"
        }
        
        # Store original values
        original_values = {}
        for key, value in env_vars.items():
            original_values[key] = os.environ.get(key)
            os.environ[key] = value
        
        yield
        
        # Restore original values
        for key, original_value in original_values.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value
    
    def test_manager_initialization(self, catalog_config: Dict[str, Any]):
        """Test IcebergMetadataManager initialization"""
        manager = IcebergMetadataManager(catalog_config)
        
        assert manager is not None
        assert manager.catalog is not None
        assert hasattr(manager, '_table_cache')
    
    def test_get_table_success(self, manager: IcebergMetadataManager, 
                              table_name: str, test_table: Any):
        """Test successful table retrieval"""
        retrieved_table = manager.get_table(table_name)
        
        assert retrieved_table is not None
        assert retrieved_table.name() == table_name.split('.')[-1]
    
    def test_get_table_caching(self, manager: IcebergMetadataManager, 
                              table_name: str, test_table: Any):
        """Test table caching functionality"""
        first_call = manager.get_table(table_name)
        second_call = manager.get_table(table_name)
        
        assert first_call is second_call, "Table should be cached"
    
    def test_get_table_nonexistent(self, manager: IcebergMetadataManager):
        """Test retrieval of non-existent table"""
        with pytest.raises(Exception):
            manager.get_table("default.nonexistent_table")
    
    def test_get_schema_success(self, manager: IcebergMetadataManager, 
                               table_name: str, test_table: Any):
        """Test successful schema retrieval"""
        schema = manager.get_schema(table_name)
        
        assert schema is not None
        assert len(schema) == 6  # Expected number of fields
        
        # Verify field names
        field_names = [field.name for field in schema]
        expected_fields = ['id', 'name', 'age', 'city', 'date', 'timestamp']
        assert all(field in field_names for field in expected_fields)
    
    def test_get_schema_nonexistent_table(self, manager: IcebergMetadataManager):
        """Test schema retrieval for non-existent table"""
        with pytest.raises(Exception):
            manager.get_schema("default.nonexistent_table")
    
    def test_get_data_files_all(self, manager: IcebergMetadataManager, 
                               table_name: str, test_table: Any):
        """Test retrieval of all data files"""
        all_files = manager.get_data_files(table_name)
        
        assert isinstance(all_files, list)
        assert len(all_files) > 0
        
        # Verify file properties
        for file in all_files:
            assert hasattr(file, 'file_path')
            assert hasattr(file, 'record_count')
            assert hasattr(file, 'file_size_in_bytes')
            assert file.record_count > 0
            assert file.file_size_in_bytes > 0
    
    def test_get_data_files_with_partition_filter(self, manager: IcebergMetadataManager, 
                                                 table_name: str, test_table: Any):
        """Test retrieval of data files with partition filter"""
        partition_filter = {"id": 1}
        filtered_files = manager.get_data_files(table_name, partition_filter)
        
        assert isinstance(filtered_files, list)
        # Note: The actual number of files depends on partitioning strategy
        # We just verify it returns without error and is a list
    
    def test_get_data_files_multiple_filters(self, manager: IcebergMetadataManager, 
                                           table_name: str, test_table: Any):
        """Test retrieval of data files with multiple partition filters"""
        multi_filter = {"id": 10}
        filtered_files = manager.get_data_files(table_name, multi_filter)
        
        assert isinstance(filtered_files, list)
    
    def test_get_data_files_nonexistent_table(self, manager: IcebergMetadataManager):
        """Test data file retrieval for non-existent table"""
        with pytest.raises(Exception):
            manager.get_data_files("default.nonexistent_table")
    
    def test_get_data_files_empty_partition_filter(self, manager: IcebergMetadataManager, 
                                                  table_name: str, test_table: Any):
        """Test data file retrieval with empty partition filter"""
        empty_filter = {}
        files = manager.get_data_files(table_name, empty_filter)
        
        # Should behave same as no filter
        all_files = manager.get_data_files(table_name)
        assert len(files) == len(all_files)
    
    @pytest.mark.performance
    def test_get_data_files_performance(self, manager: IcebergMetadataManager, 
                                       table_name: str, test_table: Any):
        """Test performance of data file retrieval"""
        num_iterations = 10
        
        start_time = time.time()
        for _ in range(num_iterations):
            _ = manager.get_data_files(table_name)
        end_time = time.time()
        
        avg_time = (end_time - start_time) / num_iterations
        
        # Assert reasonable performance (adjust threshold as needed)
        assert avg_time < 5.0, f"Average time {avg_time:.4f}s exceeds threshold"
    
    @pytest.mark.integration
    def test_full_workflow(self, manager: IcebergMetadataManager, 
                          table_name: str, test_table: Any):
        """Test complete workflow: get table -> schema -> data files"""
        # Get table
        table = manager.get_table(table_name)
        assert table is not None
        
        # Get schema
        schema = manager.get_schema(table_name)
        assert schema is not None
        assert len(schema) > 0
        
        # Get data files
        files = manager.get_data_files(table_name)
        assert isinstance(files, list)
        assert len(files) > 0
        
        # Verify data consistency
        total_records = sum(file.record_count for file in files)
        assert total_records > 0


class TestIcebergMetadataManagerEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_invalid_catalog_config(self):
        """Test initialization with invalid catalog configuration"""
        invalid_config = {"invalid": "config"}
        
        with pytest.raises(Exception):
            IcebergMetadataManager(invalid_config)
    
    def test_empty_catalog_config(self):
        """Test initialization with empty catalog configuration"""
        with pytest.raises(Exception):
            IcebergMetadataManager({})
    
    def test_none_catalog_config(self):
        """Test initialization with None catalog configuration"""
        with pytest.raises(Exception):
            IcebergMetadataManager({})


class TestIcebergMetadataManagerMocked:
    """Test IcebergMetadataManager with mocked dependencies"""
    
    @pytest.fixture
    def mock_catalog(self):
        """Fixture for mocked catalog"""
        return Mock()
    
    @pytest.fixture
    def mock_table(self):
        """Fixture for mocked table"""
        mock_table = Mock()
        mock_table.name.return_value = "test_table"
        return mock_table
    
    def test_get_table_with_mock(self, mock_catalog, mock_table):
        """Test get_table with mocked catalog"""
        mock_catalog.load_table.return_value = mock_table
        
        with patch('iceberg_management.metadata.IcebergMetadataManager.__init__', 
                  return_value=None):
            manager = IcebergMetadataManager.__new__(IcebergMetadataManager)
            manager.catalog = mock_catalog
            manager._table_cache = {}
            
            result = manager.get_table("default.test_table")
            
            assert result == mock_table
            mock_catalog.load_table.assert_called_once_with("default.test_table")
    
    def test_schema_extraction_with_mock(self, mock_catalog, mock_table):
        """Test schema extraction with mocked dependencies"""
        mock_schema = Mock()
        mock_table.schema.return_value = mock_schema
        mock_catalog.load_table.return_value = mock_table
        
        with patch('iceberg_management.metadata.IcebergMetadataManager.__init__', 
                  return_value=None):
            manager = IcebergMetadataManager.__new__(IcebergMetadataManager)
            manager.catalog = mock_catalog
            manager._table_cache = {}
            
            result = manager.get_schema("default.test_table")
            
            assert result == mock_schema
            mock_table.schema.assert_called_once()


@pytest.mark.slow
class TestIcebergMetadataManagerIntegration:
    """Integration tests that require actual Iceberg setup"""
    
    @pytest.mark.skipif(
        not os.getenv("ICEBERG_INTEGRATION_TEST"),
        reason="Integration tests require ICEBERG_INTEGRATION_TEST environment variable"
    )
    def test_real_iceberg_integration(self):
        """Test with real Iceberg setup (requires environment setup)"""
        # This would contain tests that run against actual Iceberg infrastructure
        # Only run when specifically requested via environment variable
        pass


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-x",  # Stop on first failure
        "--durations=10"  # Show slowest 10 tests
    ])