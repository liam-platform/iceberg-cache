import pyarrow as pa
from datafusion import SessionContext
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

from core.cache_data_model import PartitionInfo, CacheLocation

class DatafusionError(Exception):
    """
    Custom exception to wrap DataFusion-related errors.
    Attributes:
        message (str): A human-readable error message.
        original_exception (Exception, optional): The original exception that triggered this one.
    """
    
    def __init__(self, message: str, original_exception: Exception):
        super().__init__(message)
        self.message = message
        self.original_exception = original_exception

    def __str__(self):
        if self.original_exception:
            return f"{self.message} (Caused by: {repr(self.original_exception)})"
        return self.message

    def __repr__(self):
        return f"{self.__class__.__name__}(message={self.message!r}, original_exception={self.original_exception!r})"


@dataclass
class DistributedQueryPlan:
    """Enhanced query plan for distributed execution"""
    query_id: str
    table_references: Set[str]
    predicates: List[Dict[str, Any]]
    projections: List[str]
    required_partitions: Dict[str, List[PartitionInfo]]
    partition_locations: Dict[str, List[CacheLocation]]  # partition_key -> locations
    local_partitions: Dict[str, List[PartitionInfo]]     # Available locally
    remote_partitions: Dict[str, List[PartitionInfo]]    # Need remote fetch
    missing_partitions: Dict[str, List[PartitionInfo]]   # Need object store load
    execution_nodes: Set[str] = field(default_factory=set)
    estimated_cost: float = 0.0


class QueryEngine:
    def __init__(self, cache_node) -> None:
        self.cache_node = cache_node
        self.ctx = SessionContext()
    
    def _safe_register_view(self, table_name: str, arrow_table: pa.Table) -> None:
        df = self.ctx.from_arrow(arrow_table, table_name)
        try:
            self.ctx.register_view(table_name, df)
        except Exception as e:
            # Attempt to deregister and retry
            if "already exists" in str(e):
                self.ctx.deregister_table(table_name)
                self.ctx.register_view(table_name, df)
            else:
                raise DatafusionError("Failed to register view", e)
    
    def _register_table(self, table_name, arrow_table) -> None:
        self._safe_register_view(table_name, arrow_table)

    def execute_query(self, sql: str) -> pa.Table:
        for name, table in self.cache_node.get_all_tables().items():
            self._safe_register_view(name, table)

        df = self.ctx.sql(sql)
        return df.to_arrow_table()
