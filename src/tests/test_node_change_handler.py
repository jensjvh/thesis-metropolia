import unittest
import sys
import os
import asyncio
import time
from datetime import datetime
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from run import NodeChangeHandler
from config import PLCConfig

class TestNodeChangeHandler(unittest.TestCase):
    def setUp(self):
        self.db_mock = MagicMock()
        self.config = PLCConfig(
            name="test_plc",
            url="opc.tcp://localhost:4840",
            value_deadband_percent=1.0,
            value_deadband_absolute=2.0
        )
        self.handler = NodeChangeHandler("test_plc", self.db_mock, self.config)
        self.node_id = "ns=2;i=2"
        self.node_name = "Test Variable"
        self.handler.add_node(self.node_id, self.node_name)

    def test_add_node(self):
        node_id = "ns=2;i=3"
        name = "Another Variable"
        self.handler.add_node(node_id, name)
        self.assertEqual(self.handler.node_names[node_id], name)

    def test_should_record_first_value(self):
        self.assertTrue(self.handler.should_record_value(self.node_id, 10.0))
        self.assertEqual(self.handler.last_values[self.node_id], 10.0)

    def test_should_record_significant_percent_change(self):
        self.handler.last_values[self.node_id] = 100.0
        
        self.assertTrue(self.handler.should_record_value(self.node_id, 101.0))
        self.assertEqual(self.handler.last_values[self.node_id], 101.0)
        
        self.handler.last_values[self.node_id] = 100.0
        self.assertFalse(self.handler.should_record_value(self.node_id, 100.5))
        self.assertEqual(self.handler.last_values[self.node_id], 100.0)

    def test_should_record_significant_absolute_change(self):
        self.handler.last_values[self.node_id] = 10.0
        
        self.assertTrue(self.handler.should_record_value(self.node_id, 12.0))
        self.assertEqual(self.handler.last_values[self.node_id], 12.0)
        
        self.handler.last_values[self.node_id] = 10.0
        self.assertFalse(self.handler.should_record_value(self.node_id, 11.9))
        self.assertEqual(self.handler.last_values[self.node_id], 10.0)
    
    def test_should_record_type_change(self):
        self.handler.last_values[self.node_id] = 10.0
        self.assertTrue(self.handler.should_record_value(self.node_id, "string value"))
        self.assertEqual(self.handler.last_values[self.node_id], "string value")

    def test_should_record_boolean_change(self):
        self.handler.last_values[self.node_id] = True
        self.assertTrue(self.handler.should_record_value(self.node_id, False))
        self.assertEqual(self.handler.last_values[self.node_id], False)
        
        self.assertFalse(self.handler.should_record_value(self.node_id, False))

    def test_should_record_string_change(self):
        self.handler.last_values[self.node_id] = "old value"
        self.assertTrue(self.handler.should_record_value(self.node_id, "new value"))
        self.assertEqual(self.handler.last_values[self.node_id], "new value")
        
        self.assertFalse(self.handler.should_record_value(self.node_id, "new value"))

    def test_should_record_none_value(self):
        self.handler.last_values[self.node_id] = 10.0
        self.assertTrue(self.handler.should_record_value(self.node_id, None))
        self.assertEqual(self.handler.last_values[self.node_id], None)
        
        self.assertFalse(self.handler.should_record_value(self.node_id, None))
        
    def test_datachange_notification(self):
        node_mock = MagicMock()
        node_mock.nodeid.to_string.return_value = self.node_id
        
        data_mock = MagicMock()
        data_mock.monitored_item.Value.SourceTimestamp = datetime.now()
        data_mock.monitored_item.Value.ServerTimestamp = None
        
        self.handler.last_values[self.node_id] = 100.0
        self.handler.datachange_notification(node_mock, 105.0, data_mock)
        
        self.assertEqual(len(self.handler.data_buffer), 1)
        self.assertEqual(self.handler.data_buffer[0]["plc_name"], "test_plc")
        self.assertEqual(self.handler.data_buffer[0]["node_id"], self.node_id)
        self.assertEqual(self.handler.data_buffer[0]["node_name"], self.node_name)
        self.assertEqual(self.handler.data_buffer[0]["value"], 105.0)

    def test_flush_buffer(self):
        self.handler.data_buffer = [
            {"plc_name": "test_plc", "node_id": self.node_id, "node_name": self.node_name, "value": 42.0},
            {"plc_name": "test_plc", "node_id": self.node_id, "node_name": self.node_name, "value": 43.0}
        ]
        
        self.handler.flush_buffer()
        
        self.db_mock.store_data_batch.assert_called_once_with(self.handler.data_buffer)
        
        self.assertEqual(len(self.handler.data_buffer), 0)
        
    def test_buffer_size_trigger(self):
        self.config.buffer_size = 2
        
        node_mock = MagicMock()
        node_mock.nodeid.to_string.return_value = self.node_id
        
        data_mock = MagicMock()
        data_mock.monitored_item.Value.SourceTimestamp = datetime.now()
        
        self.handler.datachange_notification(node_mock, 10.0, data_mock)
        self.assertEqual(len(self.handler.data_buffer), 1)
        self.db_mock.store_data_batch.assert_not_called()
        
        with patch.object(self.handler, 'flush_buffer', wraps=self.handler.flush_buffer) as mock_flush:
            self.handler.datachange_notification(node_mock, 20.0, data_mock)
            mock_flush.assert_called_once()
    
    def test_buffer_time_trigger(self):
        self.config.buffer_flush_interval = 0.1
        
        node_mock = MagicMock()
        node_mock.nodeid.to_string.return_value = self.node_id
        
        data_mock = MagicMock()
        data_mock.monitored_item.Value.SourceTimestamp = datetime.now()
        
        self.handler.last_flush_time = time.time() - 0.2
        
        with patch.object(self.handler, 'flush_buffer', wraps=self.handler.flush_buffer) as mock_flush:
            self.handler.datachange_notification(node_mock, 10.0, data_mock)
            mock_flush.assert_called_once()


if __name__ == '__main__':
    unittest.main()