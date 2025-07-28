import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from core.cache_data_model import PartitionInfo
from storage.provider import ObjectStoreProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3DataLoader(ObjectStoreProvider):
    """S3DataLoader loads the parquet data from S3 into Arrow format"""
    def __init__(self, aws_config: Dict[str, str], max_workers: int = 4) -> None:
        self.s3_client = boto3.client('s3', **aws_config)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def _load_parquet_file_sync(self, s3_path: str, columns: Optional[List[str]] = None) -> pa.Table:
        """Synchronous helper to load a single Parquet file from S3"""
        try:
            # Parse S3 path
            bucket, key = s3_path.replace('s3://', '').split('/', 1)
            
            # Download file to memory
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            
            # Read with Arrow
            table = pq.read_table(pa.BufferReader(data), columns=columns)
            logger.info(f"Loaded {len(table)} rows from {s3_path}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to load {s3_path}: {e}")
            raise
    
    async def load_partition(self, partition_info: PartitionInfo) -> pa.Table:
        """Load a partition from S3"""
        loop = asyncio.get_event_loop()
        
        # Run the synchronous S3 operation in thread pool
        table = await loop.run_in_executor(
            self.executor, 
            self._load_parquet_file_sync, 
            partition_info.file_path,
            # TODO: check if columns should be included here
            # partition_info.columns if hasattr(partition_info, 'columns') else None
        )
        
        return table

    async def load_partitions_batch(self, partitions: List[PartitionInfo]) -> Dict[str, pa.Table]:
        """Load multiple partitions in parallel"""
        if not partitions:
            return {}
        
        # Create tasks for all partitions
        tasks = []
        for partition in partitions:
            task = self.load_partition(partition)
            tasks.append((partition.partition_id, task))
        
        # Execute all tasks concurrently
        results = {}
        failed_partitions = []
        
        for partition_id, task in tasks:
            try:
                table = await task
                results[partition_id] = table
            except Exception as e:
                logger.error(f"Failed to load partition {partition_id}: {e}")
                failed_partitions.append(partition_id)
        
        if failed_partitions:
            logger.warning(f"Failed to load {len(failed_partitions)} partitions: {failed_partitions}")
        
        if not results:
            raise ValueError("No partitions loaded successfully")
        
        logger.info(f"Successfully loaded {len(results)} out of {len(partitions)} partitions")
        return results
 
    def load_parquet_file(self, s3_path: str, columns: Optional[List[str]] = None) -> pa.Table:
        """Load a single Parquet file from S3"""
        try:
            # Parse S3 path
            bucket, key = s3_path.replace('s3://', '').split('/', 1)
            
            # Download file to memory
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            
            # Read with Arrow
            table = pq.read_table(pa.BufferReader(data), columns=columns)
            logger.info(f"Loaded {len(table)} rows from {s3_path}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to load {s3_path}: {e}")
            raise
    
    def load_multiple_parquet_files(self, file_paths: List[str], columns: Optional[List[str]] = None) -> pa.Table:
        """Load multiple parquet files in parallel"""
        futures = []
        for path in file_paths:
            future = self.executor.submit(self.load_parquet_file, path, columns)
            futures.append(future)
        
        tables = []
        for future in futures:
            try:
                table = future.result(timeout=30)
                tables.append(table)
            except Exception as e:
                logger.error(f"Failed to load file: {e}")
        
        if not tables:
            raise ValueError("No tables loaded successfully")

        # Concatenate all tables
        return pa.concat_tables(tables)

    def __del__(self):
        """Cleanup thread pool on destruction"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
