import unittest
import pyarrow as pa
from sql.engine import QueryEngine

class DummyCacheNode:
    def __init__(self):
        self.tables = {}
    def get_all_tables(self):
        return self.tables
    def add_table(self, name, table):
        self.tables[name] = table

class TestSQLQueryEngine(unittest.TestCase):
    def setUp(self):
        self.cache_node = DummyCacheNode()
        self.engine = QueryEngine(self.cache_node)
        # Create a simple Arrow table
        self.table = pa.table({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        self.cache_node.add_table('test_table', self.table)

    def test_register_table(self):
        self.engine._register_table('test_table', self.table)
        # Should not raise any errors

    def test_execute_query(self):
        result = self.engine.execute_query('SELECT id, value FROM test_table WHERE id > 1')
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 2)
        self.assertListEqual(result.column('id').to_pylist(), [2, 3])
        self.assertListEqual(result.column('value').to_pylist(), ['b', 'c'])
