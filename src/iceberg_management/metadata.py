from typing import Any, Dict, Optional, List

from pyiceberg.table import Table 
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.manifest import DataFile

from iceberg_management.metadata_provider import IcebergMetadataProvider


class IcebergMetadataManager(IcebergMetadataProvider):
    """Manages Iceberg tavle metadata and file listings"""
    
    def __init__(self, catalog_config: Dict[str, Any]):
        self.catalog = load_catalog("default", **catalog_config)
        self._table_cache = {}

    def get_table(self, table_id: str) -> Table:
        """Get iceberg table with caching"""
        if table_id not in self._table_cache:
            self._table_cache[table_id] = self.catalog.load_table(table_id)
        return self._table_cache[table_id]

    def get_data_files(self, table_id: str, partition_filter: Optional[Dict] = None) -> List[DataFile]:
        """Get list of data files for a table/partition"""
        table = self.get_table(table_id)
        scan = table.scan()

        if partition_filter:
            # Apply partition filter
            filter_expr = None
            for key, value in partition_filter.items():
                condition = ds.field(key) == value
                filter_expr = condition if filter_expr is None else filter_expr & condition
                scan = scan.filter(filter_expr)
        
        files: List[DataFile] = []
        for task in scan.plan_files():
            files.append(task.file)
        return files

    def get_schema(self, table_id: str) -> pa.Schema:
        """Get Arrow schema for table"""
        table = self.get_table(table_id)
        return table.schema()

    def list_tables(self) -> Optional[List[pa.Table]]:
        """Get all iceberg tables and return as arrow Tables."""
        pass

    def get_table_stats(self, table_id: str) -> Dict[str, Any]:
        """Get statistics for a specific table"""
        table = self.get_table(table_id)
        stats = {
            "table_id": table_id,
            "schema": table.schema(),
            "location": table.location(),
            # "last_updated": table.last_updated(),
            # "record_count": table.record_count()
        }
        return stats
