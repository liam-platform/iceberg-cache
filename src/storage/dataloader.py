import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3DataLoader:
    """S3DataLoader loads the parquet data from S3 into Arrow format"""
    def __init__(self, aws_config: Dict[str, str]) -> None:
        self.s3_client = boto3.client('s3', **aws_config)
        # TODO: make the max_workers configurable
        self.executor = ThreadPoolExecutor(max_workers=4)
 
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
