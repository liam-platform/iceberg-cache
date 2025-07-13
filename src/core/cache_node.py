import json
import logging
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa

from core.cache_data_model import CacheKey
from core.lru_cache import LRUCache
from iceberg_management.metadata import IcebergMetadataManager
from storage.dataloader import S3DataLoader

logger = logging.getLogger(__name__)


class ArrowCacheNode:
    """Single Cache Node implementation."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.cache = LRUCache(max_size_bytes=config.get(
            'max_cache_size', 2*1024*1024*1024))
        self.metadata_manager = IcebergMetadataManager(config['iceberg_catalog'])
        self.data_loader = S3DataLoader(config['aws'])
    
    def _create_cache_key(self, table_id: str, partition_spec: Dict, columns: Set[str]) -> CacheKey:
        """Create cache key from parameters"""
        partition_str = json.dumps(partition_spec, sort_keys=True) if partition_spec else "{}"
        return CacheKey(table_id, partition_str, columns)
    
    def get_table_data(self, table_id: str, 
                      partition_filter: Optional[Dict] = None,
                      columns: Optional[List[str]] = None) -> pa.Table:
        """Get table data with caching"""
        
        # Create cache key
        cols_set = set(columns) if columns else set()
        cache_key = self._create_cache_key(table_id, partition_filter or {}, cols_set)
        cache_key_str = str(cache_key)
        
        # Try cache first
        cached_table = self.cache.get(cache_key_str)
        if cached_table is not None:
            return cached_table
        
        # Load from Iceberg/S3
        try:
            # Get file list from Iceberg
            files = self.metadata_manager.get_data_files(table_id, partition_filter)
            if not files:
                return pa.table({})  # Empty table
            
            # Load data
            table = self.data_loader.load_multiple_parquet_files(
                [file.file_path for file in files], 
                columns
            )
            
            # Cache the result
            self.cache.put(cache_key_str, table)
            
            return table
            
        except Exception as e:
            logger.error(f"Failed to load table data: {e}")
            raise
    
    def execute_query(self, sql: str) -> pa.Table:
        """Execute simple SQL queries (MVP implementation)"""
        # This is a very basic SQL parser for MVP
        # In production, you'd use a proper SQL engine like DuckDB or DataFusion
        
        sql = sql.strip().upper()
        
        if not sql.startswith('SELECT'):
            raise ValueError("Only SELECT queries supported in MVP")
        
        # Very basic parsing - extract table name
        # Format: SELECT * FROM table_name [WHERE conditions]
        parts = sql.split()
        if 'FROM' not in parts:
            raise ValueError("FROM clause required")
        
        from_idx = parts.index('FROM')
        table_id = parts[from_idx + 1].lower()
        
        # Load full table for MVP (no predicate pushdown yet)
        return self.get_table_data(table_id)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return self.cache.get_stats()

    def invalidate_table(self, table_id: str) -> None:
        """Invalidate all cached entries for a table"""
        keys_to_remove = []
        for key in self.cache.cache.keys():
            parts = key.split("#")
            if len(parts) == 3 and parts[0] == table_id:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            with self.cache.lock:
                if key in self.cache.cache:
                    entry = self.cache.cache[key]
                    self.cache.current_size_bytes -= entry.size_bytes
                    del self.cache.cache[key]
        
        logger.info(f"Invalidated {len(keys_to_remove)} entries for table {table_id}")
