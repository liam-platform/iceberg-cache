import pyarrow as pa
from datafusion import SessionContext


class DatafusionError(Exception):
    """Custom exception to wrap DataFusion-related errors."""
    
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
