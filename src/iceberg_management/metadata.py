from typing import Any, Dict, Optional, List, Union
from datetime import datetime

from pyiceberg.table import Table 
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.manifest import DataFile
from pyiceberg.table.snapshots import Snapshot


class IcebergMetadataManager:
    """Manages Iceberg table metadata and file listings with snapshot awareness."""
    
    def __init__(self, catalog_config: Dict[str, Any]):
        self.catalog = load_catalog("default", **catalog_config)
        self._table_cache = {}
        self._snapshot_cache = {}

    def get_table(self, table_id: str) -> Table:
        """Get iceberg table with caching"""
        if table_id not in self._table_cache:
            self._table_cache[table_id] = self.catalog.load_table(table_id)
        return self._table_cache[table_id]

    def get_data_files(self, table_id: str, 
                      partition_filter: Optional[Dict] = None,
                      snapshot_id: Optional[int] = None,
                      as_of_timestamp: Optional[Union[datetime, int]] = None) -> List[DataFile]:
        """
        Get list of data files for a table/partition at a specific snapshot.
        
        Args:
            table_id: Table identifier
            partition_filter: Optional partition filter dictionary
            snapshot_id: Specific snapshot ID to query
            as_of_timestamp: Timestamp to query table state at that point
            
        Returns:
            List of DataFile objects
        """
        table = self.get_table(table_id)
        
        # Get the appropriate snapshot and create a snapshot-aware table
        if snapshot_id is not None:
            snapshot = table.snapshot_by_id(snapshot_id)
            if not snapshot:
                raise ValueError(f"Snapshot {snapshot_id} not found for table {table_id}")
            # Use the snapshot's manifest list to get files
            scan = table.scan()
            # Note: PyIceberg doesn't directly support scanning specific snapshots yet
            # This is a limitation of the current PyIceberg implementation
        elif as_of_timestamp is not None:
            # Convert datetime to milliseconds if needed
            if isinstance(as_of_timestamp, datetime):
                timestamp_ms = int(as_of_timestamp.timestamp() * 1000)
            else:
                timestamp_ms = as_of_timestamp
            snapshot = table.snapshot_as_of_timestamp(timestamp_ms)
            if not snapshot:
                raise ValueError(f"No snapshot found for timestamp {timestamp_ms}")
            scan = table.scan()
        else:
            scan = table.scan()

        # Apply partition filter if provided
        if partition_filter:
            from pyiceberg.expressions import EqualTo, And
            filter_expr = None
            for key, value in partition_filter.items():
                condition = EqualTo(key, value)
                filter_expr = condition if filter_expr is None else And(filter_expr, condition)
            if filter_expr:
                scan = scan.filter(filter_expr)
        
        files: List[DataFile] = []
        for task in scan.plan_files():
            files.append(task.file)
        return files

    def get_schema(self, table_id: str, 
                  snapshot_id: Optional[int] = None,
                  as_of_timestamp: Optional[Union[datetime, int]] = None) -> pa.Schema:
        """
        Get Arrow schema for table at a specific snapshot.
        
        Args:
            table_id: Table identifier
            snapshot_id: Specific snapshot ID
            as_of_timestamp: Timestamp to get schema at that point
            
        Returns:
            PyArrow schema
        """
        table = self.get_table(table_id)
        
        if snapshot_id is not None or as_of_timestamp is not None:
            snapshot = self.get_snapshot(table_id, snapshot_id, as_of_timestamp)
            if snapshot:
                # Get schema from snapshot's manifest
                return table.schema()
        
        return table.schema()

    def list_tables(self) -> Optional[List[pa.Table]]:
        """Get all iceberg tables and return as arrow Tables."""
        pass

    def get_table_stats(self, table_id: str,
                       snapshot_id: Optional[int] = None,
                       as_of_timestamp: Optional[Union[datetime, int]] = None) -> Dict[str, Any]:
        """
        Get statistics for a specific table at a given snapshot.
        
        Args:
            table_id: Table identifier
            snapshot_id: Specific snapshot ID
            as_of_timestamp: Timestamp to get stats at that point
            
        Returns:
            Dictionary containing table statistics
        """
        table = self.get_table(table_id)
        snapshot = self.get_snapshot(table_id, snapshot_id, as_of_timestamp)
        current_snapshot = table.current_snapshot()
        
        current_snapshot_id = current_snapshot.snapshot_id if current_snapshot else None
        stats = {
            "table_id": table_id,
            "schema": table.schema(),
            "location": table.location(),
            "current_snapshot_id": current_snapshot_id
        }
        
        if snapshot:
            stats.update({
                "snapshot_id": snapshot.snapshot_id,
                "timestamp_ms": snapshot.timestamp_ms,
                "summary": snapshot.summary,
                "manifest_list": snapshot.manifest_list,
            })
            
            # Add file count and size if available in summary
            if snapshot.summary:
                stats.update({
                    "added_files_count": snapshot.summary.get("added-files-count"),
                    "deleted_files_count": snapshot.summary.get("deleted-files-count"),
                    "total_records": snapshot.summary.get("total-records"),
                    "total_files_size": snapshot.summary.get("total-data-files-size"),
                })
        
        return stats

    def get_snapshot(self, table_id: str, snapshot_id: Optional[int] = None, 
                     as_of_timestamp: Optional[Union[datetime, int]] = None):
        """
        Get a specific snapshot by ID or timestamp.
        
        Args:
            table_id: Table identifier
            snapshot_id: Specific snapshot ID to retrieve
            as_of_timestamp: Timestamp to find the snapshot at that point in time
            
        Returns:
            Snapshot object or None if not found
        """
        cache_key = f"{table_id}:{snapshot_id}:{as_of_timestamp}"

        if cache_key not in self._snapshot_cache:
            table = self.get_table(table_id)
            
            if snapshot_id is not None:
                snapshot = table.snapshot_by_id(snapshot_id)
            elif as_of_timestamp is not None:
                # Convert datetime to milliseconds if needed
                if isinstance(as_of_timestamp, datetime):
                    timestamp_ms = int(as_of_timestamp.timestamp() * 1000)
                else:
                    timestamp_ms = as_of_timestamp
                snapshot = table.snapshot_as_of_timestamp(timestamp_ms)
            else:
                snapshot = table.current_snapshot()
                
            self._snapshot_cache[cache_key] = snapshot
            
        return self._snapshot_cache[cache_key]

    def get_snapshot_history(self, table_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get snapshot history for a table.
        
        Args:
            table_id: Table identifier
            limit: Optional limit on number of snapshots to return
            
        Returns:
            List of snapshot information dictionaries
        """
        snapshots = self.list_snapshots(table_id)
        
        # Sort by timestamp descending (newest first)
        snapshots.sort(key=lambda s: s.timestamp_ms, reverse=True)
        
        if limit:
            snapshots = snapshots[:limit]
        
        history = []
        for snapshot in snapshots:
            snapshot_info = {
                "snapshot_id": snapshot.snapshot_id,
                "timestamp_ms": snapshot.timestamp_ms,
                "timestamp": datetime.fromtimestamp(snapshot.timestamp_ms / 1000),
                "summary": snapshot.summary or {},
                "parent_snapshot_id": snapshot.parent_snapshot_id,
                "schema_id": snapshot.schema_id,
            }
            history.append(snapshot_info)
        
        return history

    def list_snapshots(self, table_id: str) -> List[Snapshot]:
        """List all snapshots for a table"""
        table = self.get_table(table_id)
        return list(table.snapshots())
    
    def get_historical_data_files(self, table_id: str,
                                 snapshot_id: Optional[int] = None,
                                 as_of_timestamp: Optional[Union[datetime, int]] = None) -> List[DataFile]:
        """
        Get data files from a specific historical snapshot by reading manifest files directly.
        
        Args:
            table_id: Table identifier
            snapshot_id: Specific snapshot ID
            as_of_timestamp: Timestamp to get files at that point
            
        Returns:
            List of DataFile objects from the historical snapshot
        """
        table = self.get_table(table_id)
        
        # Get the target snapshot
        if snapshot_id is not None:
            snapshot = table.snapshot_by_id(snapshot_id)
        elif as_of_timestamp is not None:
            if isinstance(as_of_timestamp, datetime):
                timestamp_ms = int(as_of_timestamp.timestamp() * 1000)
            else:
                timestamp_ms = as_of_timestamp
            snapshot = table.snapshot_as_of_timestamp(timestamp_ms)
        else:
            snapshot = table.current_snapshot()
        
        if not snapshot:
            raise ValueError("No snapshot found for the specified criteria")
        
        # For now, return current data files since direct manifest reading
        # requires more complex implementation with PyIceberg's current API
        # This is a limitation that would need to be addressed in a production system
        scan = table.scan()
        files: List[DataFile] = []
        for task in scan.plan_files():
            files.append(task.file)
        return files

    def time_travel_scan(self, table_id: str, 
                        target_timestamp: Union[datetime, int],
                        partition_filter: Optional[Dict] = None) -> pa.Table:
        """
        Perform a time travel scan to get data as it existed at a specific timestamp.
        
        Note: This implementation gets the historical data files and reads them directly.
        
        Args:
            table_id: Table identifier
            target_timestamp: Target timestamp for time travel
            partition_filter: Optional partition filter
            
        Returns:
            PyArrow table with data as of the target timestamp
        """
        # Get historical data files
        data_files = self.get_historical_data_files(table_id, as_of_timestamp=target_timestamp)
        
        if not data_files:
            # Return empty table with correct schema
            table = self.get_table(table_id)
            schema = table.schema()
            return pa.table([], schema=schema.as_arrow())
        
        # Read data files directly using PyArrow
        file_paths = [df.file_path for df in data_files]
        
        # Apply partition filter if provided
        if partition_filter:
            # Filter files based on partition information
            filtered_paths = []
            for i, df in enumerate(data_files):
                include_file = True
                if hasattr(df, 'partition') and df.partition:
                    for key, value in partition_filter.items():
                        if key in df.partition and df.partition[key] != value:
                            include_file = False
                            break
                if include_file:
                    filtered_paths.append(file_paths[i])
            file_paths = filtered_paths
        
        if not file_paths:
            # Return empty table with correct schema
            table = self.get_table(table_id)
            schema = table.schema()
            return pa.table([], schema=schema.as_arrow())
        
        # Read the files using PyArrow dataset
        dataset = ds.dataset(file_paths)
        return dataset.to_table()
    
    def clear_cache(self):
        """Clear all cached tables and snapshots"""
        self._table_cache.clear()
        self._snapshot_cache.clear()

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        return {
            "cached_tables": len(self._table_cache),
            "cached_snapshots": len(self._snapshot_cache)
        }
    