import unittest
import sys
import os
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from run import PLCCollector
from config import Config, PLCConfig, DBConfig


class TestPLCCollector(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        db_config = DBConfig(
            dbname="heljens",
            user="heljens", 
            password="heljens",
            host="localhost",
            port="5432"
        )
        
        plc_config = PLCConfig(
            name="test_plc",
            url="opc.tcp://localhost:4840/freeopcua/server/",
            auto_discover=True,
            discover_depth=2
        )
        
        config = Config(
            plcs=[plc_config],
            database=db_config,
            poll_interval=0.1
        )
        
        self.db_mock = MagicMock()
        
        self.collector = PLCCollector(config)
        self.collector.db = self.db_mock
        
        self.client_mock = AsyncMock()
        self.node_mock = AsyncMock()
        
        self.client_mock.nodes.root = self.node_mock
        
        self.plc_config = self.collector.config.plcs[0]
    
    @patch('run.Client')
    async def test_connect_to_plc(self, mock_client_class):
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        
        result = await self.collector.connect_to_plc(self.plc_config)
        
        mock_client_class.assert_called_with(self.plc_config.url, timeout=10)
        
        mock_client.connect.assert_called_once()
        
        self.assertEqual(result, mock_client)
    
    @patch('run.Client')
    async def test_connect_to_plc_failure(self, mock_client_class):
        mock_client = AsyncMock()
        mock_client.connect.side_effect = Exception("Connection refused")
        mock_client_class.return_value = mock_client
        
        with patch('run.PLCCollector.connect_to_plc', PLCCollector.connect_to_plc):
            result = await self.collector.connect_to_plc(self.plc_config)
        
        mock_client_class.assert_called()
        
        self.assertGreaterEqual(mock_client.connect.call_count, 1)
        
        self.assertIsNone(result)
    
    async def test_browse_nodes(self):
        node = AsyncMock()
        node.nodeid.to_string.return_value = "ns=2;i=1"
        node.read_node_class.return_value = 2
        node.read_display_name.return_value = MagicMock(Text="Temperature")
        node.read_browse_name.return_value = MagicMock(Name="Temperature")
        node.read_data_type.return_value = "ns=0;i=10"
        node.get_children.return_value = []
        
        client = AsyncMock()
        data_type_node = AsyncMock()
        data_type_node.read_browse_name.return_value = MagicMock(Name="Float")
        client.get_node.return_value = data_type_node
        
        discovered = {}
        visited = set()
        
        await self.collector._browse_nodes(
            client, node, discovered, depth=0, max_depth=1, 
            throttle_ms=0, discover_types=None, visited_nodes=visited
        )
        
        self.assertEqual(len(discovered), 1)
        self.assertIn("ns=2;i=1", discovered)
        self.assertEqual(discovered["ns=2;i=1"], "Temperature")
    
    async def test_discover_nodes(self):
        client = AsyncMock()
        root_node = AsyncMock()
        objects_node = AsyncMock()
        
        client.nodes.root = root_node
        root_node.get_child.return_value = objects_node
        
        async def mock_browse_nodes(client, node, discovered, **kwargs):
            discovered["ns=2;i=1"] = "Temperature"
            
        with patch.object(self.collector, '_browse_nodes', mock_browse_nodes):
            result = await self.collector.discover_nodes(self.plc_config, client)
        
        self.assertEqual(len(result), 1)
        self.assertIn("ns=2;i=1", result)
        
        self.assertIn("ns=2;i=1", self.plc_config.nodes)
    
    async def test_setup_subscriptions(self):
        client = AsyncMock()
        subscription_mock = AsyncMock()
        handle_mock = MagicMock()
        
        client.create_subscription.return_value = subscription_mock
        subscription_mock.subscribe_data_change.return_value = handle_mock
        
        self.plc_config.nodes = {"ns=2;i=1": "Temperature"}
        
        subscription, handles = await self.collector.setup_subscriptions(self.plc_config, client)
        
        client.create_subscription.assert_called_once()
        
        subscription_mock.subscribe_data_change.assert_called_once()
        
        self.assertEqual(subscription, subscription_mock)
        self.assertEqual(len(handles), 1)
        self.assertEqual(handles[0], handle_mock)
    
    def test_save_discovery_cache(self):
        discovered = {"ns=2;i=1": "Temperature", "ns=2;i=2": "Pressure"}
        
        mock_open = unittest.mock.mock_open()
        
        with patch('builtins.open', mock_open):
            with patch('json.dump') as mock_dump:
                with patch('os.makedirs') as mock_makedirs:
                    self.collector._save_discovery_cache("test_plc", discovered)
        
        mock_makedirs.assert_called_once()
        
        mock_open.assert_called_once()
        
        mock_dump.assert_called_once()
        args, kwargs = mock_dump.call_args
        self.assertEqual(args[0], discovered)


if __name__ == '__main__':
    unittest.main()