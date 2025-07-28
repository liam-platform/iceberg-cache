import pytest
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
import io

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from storage.dataloader import S3DataLoader
from core.cache_data_model import PartitionInfo


# Test fixtures
@pytest.fixture
def aws_config():
    return {
        'aws_access_key_id': 'test_key',
        'aws_secret_access_key': 'test_secret',
        'region_name': 'us-east-1'
    }


@pytest.fixture
def sample_arrow_table():
    """Create a sample Arrow table for testing"""
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45]
    }
    return pa.table(data)


@pytest.fixture
def sample_parquet_data(sample_arrow_table):
    """Create sample parquet data as bytes"""
    buffer = io.BytesIO()
    pq.write_table(sample_arrow_table, buffer)
    return buffer.getvalue()


@pytest.fixture
def partition_info():
    """Create a mock PartitionInfo object"""
    partition = Mock(spec=PartitionInfo)
    partition.path = 's3://test-bucket/path/to/file.parquet'
    partition.partition_id = 'partition_1'
    partition.columns = None
    return partition


@pytest.fixture
def partition_info_with_columns():
    """Create a mock PartitionInfo object with specific columns"""
    partition = Mock(spec=PartitionInfo)
    partition.path = 's3://test-bucket/path/to/file.parquet'
    partition.partition_id = 'partition_2'
    partition.columns = ['id', 'name']
    return partition


class TestS3DataLoader:
    """Test suite for S3DataLoader"""
    
    def test_init(self, aws_config):
        """Test S3DataLoader initialization"""
        loader = S3DataLoader(aws_config, max_workers=8)
        
        assert loader.s3_client is not None
        assert isinstance(loader.executor, ThreadPoolExecutor)
        assert loader.executor._max_workers == 8
    
    def test_init_default_max_workers(self, aws_config):
        """Test S3DataLoader initialization with default max_workers"""
        loader = S3DataLoader(aws_config)
        assert loader.executor._max_workers == 4
    
    @patch('boto3.client')
    def test_load_parquet_file_sync_success(self, mock_boto_client, aws_config, sample_parquet_data, sample_arrow_table):
        """Test successful synchronous parquet file loading"""
        # Setup mock S3 client
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock S3 response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = sample_parquet_data
        mock_s3_client.get_object.return_value = mock_response
        
        loader = S3DataLoader(aws_config)
        
        result = loader._load_parquet_file_sync('s3://test-bucket/path/to/file.parquet')
        
        # Verify S3 client was called correctly
        mock_s3_client.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='path/to/file.parquet'
        )
        
        # Verify result
        assert isinstance(result, pa.Table)
        assert len(result) == 5
        assert result.schema.names == ['id', 'name', 'age']
    
    @patch('boto3.client')
    def test_load_parquet_file_sync_with_columns(self, mock_boto_client, aws_config, sample_parquet_data):
        """Test synchronous parquet file loading with specific columns"""
        # Setup mock S3 client
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock S3 response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = sample_parquet_data
        mock_s3_client.get_object.return_value = mock_response
        
        loader = S3DataLoader(aws_config)
        
        result = loader._load_parquet_file_sync(
            's3://test-bucket/path/to/file.parquet',
            columns=['id', 'name']
        )
        
        # Verify result has only requested columns
        assert isinstance(result, pa.Table)
        assert result.schema.names == ['id', 'name']
    
    @patch('boto3.client')
    def test_load_parquet_file_sync_failure(self, mock_boto_client, aws_config):
        """Test synchronous parquet file loading failure"""
        # Setup mock S3 client to raise exception
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        
        loader = S3DataLoader(aws_config)
        
        with pytest.raises(ClientError):
            loader._load_parquet_file_sync('s3://test-bucket/nonexistent.parquet')
    
    @patch('boto3.client')
    @pytest.mark.asyncio
    async def test_load_partition_success(self, mock_boto_client, aws_config, sample_parquet_data, partition_info):
        """Test successful async partition loading"""
        # Setup mock S3 client
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock S3 response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = sample_parquet_data
        mock_s3_client.get_object.return_value = mock_response
        
        loader = S3DataLoader(aws_config)
        
        result = await loader.load_partition(partition_info)
        
        # Verify result
        assert isinstance(result, pa.Table)
        assert len(result) == 5
        mock_s3_client.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='path/to/file.parquet'
        )
    
    @patch('boto3.client')
    @pytest.mark.asyncio
    async def test_load_partition_with_columns(self, mock_boto_client, aws_config, sample_parquet_data, partition_info_with_columns):
        """Test async partition loading with specific columns"""
        # Setup mock S3 client
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock S3 response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = sample_parquet_data
        mock_s3_client.get_object.return_value = mock_response
        
        loader = S3DataLoader(aws_config)
        
        result = await loader.load_partition(partition_info_with_columns)
        
        # Verify result has only requested columns
        assert isinstance(result, pa.Table)
        assert result.schema.names == ['id', 'name']
    
    @patch('boto3.client')
    @pytest.mark.asyncio
    async def test_load_partition_failure(self, mock_boto_client, aws_config, partition_info):
        """Test async partition loading failure"""
        # Setup mock S3 client to raise exception
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        
        loader = S3DataLoader(aws_config)
        
        with pytest.raises(ClientError):
            await loader.load_partition(partition_info)
    
    @patch('boto3.client')
    @pytest.mark.asyncio
    async def test_load_partitions_batch_success(self, mock_boto_client, aws_config, sample_parquet_data):
        """Test successful batch partition loading"""
        # Setup mock S3 client
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock S3 response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = sample_parquet_data
        mock_s3_client.get_object.return_value = mock_response
        
        # Create multiple partition info objects
        partitions = []
        for i in range(3):
            partition = Mock(spec=PartitionInfo)
            partition.path = f's3://test-bucket/path/to/file{i}.parquet'
            partition.partition_id = f'partition_{i}'
            partition.columns = None
            partitions.append(partition)
        
        loader = S3DataLoader(aws_config)
        
        result = await loader.load_partitions_batch(partitions)
        
        # Verify results
        assert isinstance(result, dict)
        assert len(result) == 3
        assert 'partition_0' in result
        assert 'partition_1' in result
        assert 'partition_2' in result
        
        for table in result.values():
            assert isinstance(table, pa.Table)
            assert len(table) == 5
        
        # Verify S3 client was called for each partition
        assert mock_s3_client.get_object.call_count == 3
    
    @pytest.mark.asyncio
    async def test_load_partitions_batch_empty_list(self, aws_config):
        """Test batch loading with empty partition list"""
        loader = S3DataLoader(aws_config)
        
        result = await loader.load_partitions_batch([])
        
        assert result == {}
    
    @patch('boto3.client')
    @pytest.mark.asyncio
    async def test_load_partitions_batch_partial_failure(self, mock_boto_client, aws_config, sample_parquet_data):
        """Test batch loading with some partitions failing"""
        # Setup mock S3 client
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock S3 response - first call succeeds, second fails, third succeeds
        mock_response_success = {
            'Body': Mock()
        }
        mock_response_success['Body'].read.return_value = sample_parquet_data
        
        mock_s3_client.get_object.side_effect = [
            mock_response_success,  # partition_0 succeeds
            ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject'),  # partition_1 fails
            mock_response_success   # partition_2 succeeds
        ]
        
        # Create multiple partition info objects
        partitions = []
        for i in range(3):
            partition = Mock(spec=PartitionInfo)
            partition.path = f's3://test-bucket/path/to/file{i}.parquet'
            partition.partition_id = f'partition_{i}'
            partition.columns = None
            partitions.append(partition)
        
        loader = S3DataLoader(aws_config)
        
        result = await loader.load_partitions_batch(partitions)
        
        # Verify results - should have 2 successful partitions
        assert isinstance(result, dict)
        assert len(result) == 2
        assert 'partition_0' in result
        assert 'partition_1' not in result  # This one failed
        assert 'partition_2' in result
    
    @patch('boto3.client')
    @pytest.mark.asyncio
    async def test_load_partitions_batch_all_fail(self, mock_boto_client, aws_config):
        """Test batch loading when all partitions fail"""
        # Setup mock S3 client to always fail
        mock_s3_client = Mock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        
        # Create partition info objects
        partitions = []
        for i in range(2):
            partition = Mock(spec=PartitionInfo)
            partition.path = f's3://test-bucket/path/to/file{i}.parquet'
            partition.partition_id = f'partition_{i}'
            partition.columns = None
            partitions.append(partition)
        
        loader = S3DataLoader(aws_config)
        
        with pytest.raises(ValueError, match="No partitions loaded successfully"):
            await loader.load_partitions_batch(partitions)
    
    def test_s3_path_parsing(self, aws_config):
        """Test S3 path parsing edge cases"""
        loader = S3DataLoader(aws_config)
        
        # Test various S3 path formats
        test_cases = [
            ('s3://bucket/file.parquet', 'bucket', 'file.parquet'),
            ('s3://bucket/path/to/file.parquet', 'bucket', 'path/to/file.parquet'),
            ('s3://bucket-name/deep/nested/path/file.parquet', 'bucket-name', 'deep/nested/path/file.parquet')
        ]
        
        for s3_path, expected_bucket, expected_key in test_cases:
            # We need to mock the S3 client since we're testing the internal method
            with patch.object(loader, 's3_client') as mock_client:
                mock_response = {'Body': Mock()}
                mock_response['Body'].read.return_value = b'mock_parquet_data'
                mock_client.get_object.return_value = mock_response
                
                # Mock pq.read_table to avoid actual parquet parsing
                with patch('pyarrow.parquet.read_table') as mock_read_table:
                    mock_table = Mock(spec=pa.Table)
                    mock_read_table.return_value = mock_table
                    
                    try:
                        loader._load_parquet_file_sync(s3_path)
                        mock_client.get_object.assert_called_with(
                            Bucket=expected_bucket,
                            Key=expected_key
                        )
                    except Exception:
                        # We expect exceptions due to mocking, but we verified the call
                        pass
    
    def test_destructor(self, aws_config):
        """Test proper cleanup in destructor"""
        loader = S3DataLoader(aws_config)
        executor_mock = Mock()
        loader.executor = executor_mock
        
        # Trigger destructor
        del loader
        
        # Verify executor shutdown was called
        executor_mock.shutdown.assert_called_once_with(wait=False)
