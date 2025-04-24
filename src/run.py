import asyncio
import logging
import signal
import sys
from asyncua import ua, Client
from asyncua.common.subscription import DataChangeNotif
from typing import List, Dict, Any, Optional, Tuple, Set
import os
import time

from config import Config, PLCConfig, load_config
from db import Database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("plc_collector.log")
    ]
)
logger = logging.getLogger(__name__)

PUBLISHING_INTERVAL = 500

class NodeChangeHandler:
    def __init__(self, plc_name, db, config):
        self.plc_name = plc_name
        self.db = db
        self.config = config
        self.node_names = {}
        self.last_values = {}
        self.data_buffer = []
        self.last_flush_time = time.time()
        
    def add_node(self, node_id, node_name):
        self.node_names[node_id] = node_name
    
    def should_record_value(self, node_id, current_value):
        if node_id not in self.last_values:
            self.last_values[node_id] = current_value
            return True
            
        last_value = self.last_values[node_id]
        
        if last_value is None or current_value is None:
            return last_value != current_value
            
        if type(last_value) != type(current_value):
            self.last_values[node_id] = current_value
            return True
            
        if isinstance(current_value, (int, float)):
            deadband_percent = getattr(self.config, 'value_deadband_percent', 1.0)
            deadband_absolute = getattr(self.config, 'value_deadband_absolute', 0.0)
            
            if last_value != 0:
                relative_change = abs((current_value - last_value) / last_value) * 100
                if relative_change >= deadband_percent:
                    self.last_values[node_id] = current_value
                    return True
                    
            if deadband_absolute > 0 and abs(current_value - last_value) >= deadband_absolute:
                self.last_values[node_id] = current_value
                return True
                
            return False
        
        if current_value != last_value:
            self.last_values[node_id] = current_value
            return True
            
        return False
    
    def datachange_notification(self, node, val, data):
        try:
            node_id = node.nodeid.to_string()
            node_name = self.node_names.get(node_id, "Unknown")
            
            if self.should_record_value(node_id, val):
                logger.debug(f"Recording change for {node_name} ({node_id}): {val}")

                self.data_buffer.append({
                    "plc_name": self.plc_name,
                    "node_id": node_id,
                    "node_name": node_name,
                    "value": val,
                    "timestamp": data.monitored_item.Value.SourceTimestamp or data.monitored_item.Value.ServerTimestamp
                })
                
                buffer_size = getattr(self.config, 'buffer_size', 100)
                buffer_flush_interval = getattr(self.config, 'buffer_flush_interval', 5)
                
                current_time = time.time()
                if (len(self.data_buffer) >= buffer_size or 
                    current_time - self.last_flush_time >= buffer_flush_interval):
                    self.flush_buffer()
            else:
                logger.debug(f"Skipping insignificant change for {node_name} ({node_id}): {val}")
                
        except Exception as e:
            logger.error(f"Error handling subscription notification: {e}")
    
    def flush_buffer(self):
        if self.data_buffer:
            try:
                self.db.store_data_batch(self.data_buffer)
                logger.debug(f"Flushed {len(self.data_buffer)} records to database")
            except Exception as e:
                logger.error(f"Error flushing data buffer: {e}")
            finally:
                self.data_buffer = []
                self.last_flush_time = time.time()
    
    def event_notification(self, event):
        logger.debug(f"Event notification: {event}")

class PLCCollector:
    def __init__(self, config: Config):
        self.config = config
        self.db = Database(config.database)
        self.running = False
        self.tasks = []
    
    def setup(self):
        self.db.setup()
        logger.info("PLC Collector setup complete")

    def _save_discovery_cache(self, plc_name, discovered_nodes):
        cache_dir = os.path.join(os.path.dirname(__file__), ".cache")
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, f"{plc_name}_nodes.cache")
        try:
            with open(cache_file, 'w') as f:
                import json
                json.dump(discovered_nodes, f)
            logger.debug(f"Saved node discovery cache to {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save discovery cache: {e}")
    
    async def connect_to_plc(self, plc_config: PLCConfig):
        retry_count = 0
        max_retries = 5
        retry_delay = 5
        
        while retry_count < max_retries:
            try:
                client = Client(plc_config.url, timeout=10)
                
                if hasattr(plc_config, 'username') and plc_config.username:
                    client.set_user(plc_config.username)
                    client.set_password(plc_config.password)
                
                app_name = f"PLC Collector - {plc_config.name}"
                client.application_uri = f"urn:plc:collector:{plc_config.name}"
                
                security_policy = getattr(plc_config, 'security_policy', None)
                if security_policy:
                    await client.set_security_string(security_policy)
                
                await client.connect()
                logger.info(f"Successfully connected to {plc_config.name} at {plc_config.url}")
                return client
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Connection attempt {retry_count}/{max_retries} to {plc_config.name} failed: {e}")
                await asyncio.sleep(retry_delay)
        
        logger.error(f"Failed to connect to {plc_config.name} after {max_retries} attempts")
        return None
    
    async def discover_nodes(self, plc_config: PLCConfig, client):
        logger.info(f"Discovering nodes for PLC: {plc_config.name} at {plc_config.url}")
        
        max_depth = getattr(plc_config, 'discover_depth', 3)
        throttle_ms = getattr(plc_config, 'discover_throttle_ms', 100)
        discover_paths = getattr(plc_config, 'discover_paths', ["/Objects"])
        discover_types = getattr(plc_config, 'discovery_types', None)
        
        discovered = {}
        
        try:
            for start_path in discover_paths:
                try:
                    if start_path.startswith("/"):
                        components = start_path.strip("/").split("/")
                        start_node = client.nodes.root
                        for component in components:
                            start_node = await start_node.get_child(component)
                    else:
                        start_node = client.get_node(start_path)
                        
                    logger.info(f"Starting discovery from {start_path} with depth={max_depth}")
                    
                    await self._browse_nodes(
                        client,
                        start_node,
                        discovered,
                        depth=0,
                        max_depth=max_depth,
                        throttle_ms=throttle_ms,
                        discover_types=discover_types
                    )
                        
                except Exception as e:
                    logger.error(f"Error discovering from path {start_path}: {e}")
            
            if discovered:
                plc_config.nodes.update(discovered)
                logger.info(f"Discovered {len(discovered)} nodes on {plc_config.name}")
                
                cache_time = getattr(plc_config, 'discover_cache_time', 0)
                if cache_time > 0:
                    self._save_discovery_cache(plc_config.name, discovered)
            
            return discovered
        except Exception as e:
            logger.error(f"Error during node discovery on {plc_config.name}: {e}")
            return {}
        
    async def _browse_nodes(self, client, node, discovered, depth=0, max_depth=3, throttle_ms=100, discover_types=None, visited_nodes=None):
        if depth > max_depth:
            return
            
        if visited_nodes is None:
            visited_nodes = set()
        
        try:
            node_id = node.nodeid.to_string()
            
            if node_id in visited_nodes:
                return
                
            visited_nodes.add(node_id)
            
            try:
                display_name = await node.read_display_name()
                browse_name = await node.read_browse_name()
                node_class = await node.read_node_class()
                
                if node_class == ua.NodeClass.Variable:
                    try:
                        data_type_node = await node.read_data_type()
                        data_type = await client.get_node(data_type_node).read_browse_name()
                        
                        if discover_types is None or data_type.Name in discover_types:
                            try:
                                await node.read_value()
                                node_desc = f"{browse_name.Name}" if browse_name.Name else display_name.Text
                                discovered[node_id] = node_desc
                                logger.debug(f"Discovered variable: {node_desc} ({node_id}) [Type: {data_type.Name}]")
                            except Exception as e:
                                logger.debug(f"Skipping node {node_id} - can't read value: {e}")
                    except Exception as e:
                        logger.debug(f"Skipping node {node_id} - can't determine data type: {e}")
                elif node_class == ua.NodeClass.Object:
                    logger.debug(f"Discovered object: {display_name.Text} ({node_id})")
                    
                if throttle_ms > 0:
                    await asyncio.sleep(throttle_ms/1000)
                    
            except Exception as e:
                logger.debug(f"Error getting node properties: {e}")
            
            if depth < max_depth:
                try:
                    children = await node.get_children()
                    sorted_children = []
                    
                    for child in children:
                        try:
                            child_node_id = child.nodeid.to_string()
                            if child_node_id in visited_nodes:
                                continue
                                
                            child_class = await child.read_node_class()
                            sorted_children.append((child, child_class))
                        except:
                            sorted_children.append((child, ua.NodeClass.Object))
                    
                    sorted_children.sort(key=lambda x: 0 if x[1] == ua.NodeClass.Variable else 1)
                    
                    max_children = 100
                    for i, (child, _) in enumerate(sorted_children):
                        if i >= max_children:
                            logger.debug(f"Limiting children exploration at node {node_id} ({max_children} limit reached)")
                            break
                            
                        await self._browse_nodes(
                            client, 
                            child, 
                            discovered, 
                            depth + 1, 
                            max_depth, 
                            throttle_ms,
                            discover_types,
                            visited_nodes
                        )
                        
                        if throttle_ms > 0:
                            await asyncio.sleep(throttle_ms/1000)
                            
                except Exception as e:
                    logger.debug(f"Error exploring children of {node_id}: {e}")
        except Exception as e:
            logger.debug(f"Error browsing node: {e}")

    async def setup_subscriptions(self, plc_config: PLCConfig, client):
        handler = NodeChangeHandler(plc_config.name, self.db, plc_config)
        
        subscription = await client.create_subscription(PUBLISHING_INTERVAL, handler)
        
        handles = []
        for node_id, node_name in plc_config.nodes.items():
            try:
                node = client.get_node(node_id)
                handler.add_node(node_id, node_name)
                handle = await subscription.subscribe_data_change(node)
                handles.append(handle)
                logger.info(f"Subscribed to {node_name} ({node_id})")
            except Exception as e:
                logger.error(f"Error subscribing to {node_id}: {e}")
        
        return subscription, handles
    
    async def collect_from_plc(self, plc_config: PLCConfig):
        logger.info(f"Starting collection from PLC: {plc_config.name} at {plc_config.url}")
        
        if plc_config.nodes is None:
            plc_config.nodes = {}
            
        use_polling = True
        reconnect_delay = 5
        
        while self.running:
            try:
                client = await self.connect_to_plc(plc_config)
                if not client:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 1.5, 30)
                    continue
                    
                reconnect_delay = 5
                    
                if getattr(plc_config, 'auto_discover', False):
                    discovered = await self.discover_nodes(plc_config, client)
                
                try:
                    if plc_config.nodes:
                        subscription, handles = await self.setup_subscriptions(plc_config, client)
                        use_polling = False
                        
                        while self.running:
                            try:
                                if plc_config.nodes:
                                    first_node_id = next(iter(plc_config.nodes))
                                    node = client.get_node(first_node_id)
                                    await node.read_browse_name()
                                await asyncio.sleep(5)
                            except Exception as e:
                                logger.warning(f"Connection to {plc_config.name} lost: {e}")
                                break
                    else:
                        logger.warning(f"No nodes found for {plc_config.name}, waiting for discovery")
                        await asyncio.sleep(30)
                        break
                        
                except Exception as e:
                    logger.warning(f"Subscription not supported for {plc_config.name}, falling back to polling: {e}")
                    use_polling = True
                
                if use_polling and plc_config.nodes:
                    while self.running:
                        batch_data = []
                        try:
                            try:
                                if plc_config.nodes:
                                    first_node_id = next(iter(plc_config.nodes))
                                    node = client.get_node(first_node_id)
                                    await node.read_browse_name()
                            except Exception:
                                logger.warning(f"Connection to {plc_config.name} lost during polling")
                                break
                                
                            for node_id, node_name in plc_config.nodes.items():
                                try:
                                    node = client.get_node(node_id)
                                    data_value = await node.read_data_value()
                                    value = data_value.Value.Value
                                    
                                    batch_data.append({
                                        "plc_name": plc_config.name,
                                        "node_id": node_id,
                                        "node_name": node_name,
                                        "value": value,
                                        "timestamp": data_value.SourceTimestamp or data_value.ServerTimestamp
                                    })
                                    
                                except Exception as node_error:
                                    logger.error(f"Error reading node {node_id} ({node_name}): {node_error}")
                                    if not batch_data:
                                        raise
                            
                            if batch_data:
                                self.db.store_data_batch(batch_data)
                                
                            await asyncio.sleep(self.config.poll_interval)
                        except Exception as e:
                            logger.error(f"Error in polling loop for {plc_config.name}: {e}")
                            break
                
                try:
                    await client.disconnect()
                except Exception:
                    pass
                                
            except Exception as connection_error:
                logger.error(f"Connection error for {plc_config.name}: {connection_error}")
                await asyncio.sleep(reconnect_delay)
        
    async def start(self):
        self.running = True
        self.setup()
        
        for plc_config in self.config.plcs:
            task = asyncio.create_task(self.collect_from_plc(plc_config))
            self.tasks.append(task)
        
        for signal_name in ('SIGINT', 'SIGTERM'):
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(
                getattr(signal, signal_name),
                lambda: asyncio.create_task(self.shutdown())
            )
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
    
    async def shutdown(self):
        logger.info("Shutting down PLC collector...")
        self.running = False
        
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("PLC collector shutdown complete")

async def main():
    try:
        config_path = os.environ.get("CONFIG_PATH", "config.yaml")
        config = load_config(config_path)
        
        collector = PLCCollector(config)
        await collector.start()
    except Exception as e:
        logger.critical(f"Critical error in main: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())