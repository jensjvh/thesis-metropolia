import unittest
import sys
import os
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db import Database
from config import DBConfig

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        self.pool_patcher = patch('psycopg2.pool.ThreadedConnectionPool')
        self.mock_pool_class = self.pool_patcher.start()
        self.mock_pool = MagicMock()
        self.mock_pool_class.return_value = self.mock_pool
        
        self.mock_pool.getconn.return_value = self.mock_connection
        
        self.db_config = DBConfig(
            dbname="heljens",
            user="heljens",
            password="heljens",
            host="localhost",
            port="5432"
        )
        
        self.db = Database(self.db_config)
        
        self.db.get_connection = MagicMock()
        self.db.get_connection.return_value.__enter__.return_value = self.mock_connection
    
    def tearDown(self):
        self.pool_patcher.stop()
    
    def test_setup_creates_tables(self):
        self.mock_cursor.fetchone.return_value = [1]
        
        self.db.setup()
        
        create_calls = [call for call in self.mock_cursor.execute.call_args_list 
                       if "CREATE TABLE" in call[0][0]]
        self.assertGreaterEqual(len(create_calls), 2)
        
        plc_data_calls = [call for call in self.mock_cursor.execute.call_args_list 
                         if "CREATE TABLE" in call[0][0] and "plc_data" in call[0][0]]
        self.assertGreaterEqual(len(plc_data_calls), 1)
        
        plc_nodes_calls = [call for call in self.mock_cursor.execute.call_args_list 
                          if "CREATE TABLE" in call[0][0] and "plc_nodes" in call[0][0]]
        self.assertGreaterEqual(len(plc_nodes_calls), 1)
        
    def test_setup_checks_timescaledb(self):
        self.mock_cursor.fetchone.side_effect = [[1], [1]]
        
        self.db.setup()
        
        timescale_calls = [call for call in self.mock_cursor.execute.call_args_list 
                          if "pg_extension" in call[0][0] and "timescaledb" in call[0][0]]
        self.assertGreaterEqual(len(timescale_calls), 1)
        
    def test_setup_creates_hypertable(self):
        self.mock_cursor.fetchone.side_effect = [[1], [1], [None]]
        
        self.db.setup()
        
        hypertable_calls = [call for call in self.mock_cursor.execute.call_args_list 
                           if "create_hypertable" in call[0][0]]
        self.assertGreaterEqual(len(hypertable_calls), 1)
        
    def test_store_data_batch_empty(self):
        self.db.store_data_batch([])
        self.mock_cursor.execute.assert_not_called()
        
    def test_store_data_batch_number(self):
        records = [
            {"plc_name": "plc1", "node_id": "ns=2;i=1", "node_name": "Temp", "value": 25.5}
        ]
        
        self.db.store_data_batch(records)
        
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        
        insert_call = self.mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn("value_number", insert_call)
        
    def test_store_data_batch_boolean(self):
        records = [
            {"plc_name": "plc1", "node_id": "ns=2;i=1", "node_name": "Switch", "value": True}
        ]
        
        self.db.store_data_batch(records)
        
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        
        insert_call = self.mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn("value_boolean", insert_call)
        
    def test_store_data_batch_string(self):
        records = [
            {"plc_name": "plc1", "node_id": "ns=2;i=1", "node_name": "Status", "value": "Running"}
        ]
        
        self.db.store_data_batch(records)
        
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        
        insert_call = self.mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn("value_string", insert_call)
        
    def test_store_data_batch_null(self):
        records = [
            {"plc_name": "plc1", "node_id": "ns=2;i=1", "node_name": "Status", "value": None}
        ]
        
        self.db.store_data_batch(records)
        
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        
    def test_store_data_batch_multiple_types(self):
        records = [
            {"plc_name": "plc1", "node_id": "ns=2;i=1", "node_name": "Temp", "value": 25.5},
            {"plc_name": "plc1", "node_id": "ns=2;i=2", "node_name": "Status", "value": "Running"},
            {"plc_name": "plc1", "node_id": "ns=2;i=3", "node_name": "Switch", "value": True}
        ]
        
        self.db.store_data_batch(records)
        
        self.assertEqual(self.mock_cursor.execute.call_count, 4)
        
        for i in range(3):
            node_call = self.mock_cursor.execute.call_args_list[i][0][0]
            self.assertIn("INSERT INTO plc_nodes", node_call)
            self.assertIn("ON CONFLICT", node_call)
        
        data_call = self.mock_cursor.execute.call_args_list[3][0][0]
        self.assertIn("INSERT INTO plc_data", data_call)
        
    def test_apply_retention_policy(self):
        self.db._apply_retention_policy(self.mock_cursor)
        
        retention_calls = [call for call in self.mock_cursor.execute.call_args_list 
                          if "drop_chunks" in call[0][0]]
        self.assertEqual(len(retention_calls), 1)
        
    def test_retention_policy_fallback(self):
        self.mock_cursor.execute.side_effect = [Exception("function drop_chunks() does not exist"), None]
        
        self.db._apply_retention_policy(self.mock_cursor)
        
        delete_calls = [call for call in self.mock_cursor.execute.call_args_list 
                       if "DELETE FROM plc_data" in call[0][0]]
        self.assertEqual(len(delete_calls), 1)
        
    def test_close(self):
        self.db.close()
        self.mock_pool.closeall.assert_called_once()


if __name__ == '__main__':
    unittest.main()