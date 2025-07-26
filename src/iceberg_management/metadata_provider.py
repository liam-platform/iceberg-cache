from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pyarrow as pa

from core.cache_data_model import PartitionInfo


class IcebergMetadataProvider(ABC):
    """Enhanced interface for Iceberg metadata with snapshot awareness"""
    
    @abstractmethod
    def get_table_metadata(self, table_name: str, snapshot_id: Optional[int] = None) -> Dict[str, Any]:
        """Get table metadata for specific snapshot"""
        pass
    
    @abstractmethod
    def get_partitions(self, table_name: str, snapshot_id: Optional[int] = None, 
                      predicates: List[Dict[str, Any]] = []) -> List[PartitionInfo]:
        """Get partition information with snapshot awareness"""
        pass
    
    @abstractmethod
    def get_partition_schema(self, table_name: str, snapshot_id: Optional[int] = None) -> pa.Schema:
        """Get the Arrow schema for the table"""
        pass
    
    @abstractmethod
    def get_current_snapshot(self, table_name: str) -> int:
        """Get current snapshot ID for table"""
        pass

