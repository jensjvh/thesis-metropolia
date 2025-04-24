import os
import yaml
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class PLCConfig:
    name: str
    url: str
    nodes: Dict[str, str] = None  # node_id: description
    auto_discover: bool = False
    discover_depth: int = 3
    discover_throttle_ms: int = 100
    discover_paths: List[str] = None
    discover_cache_time: int = 0
    discovery_types: List[str] = None
    
    def __post_init__(self):
        if self.nodes is None:
            self.nodes = {}
        if self.discover_paths is None:
            self.discover_paths = ["/Objects"]

@dataclass
class DBConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: str

@dataclass
class Config:
    plcs: List[PLCConfig]
    database: DBConfig
    poll_interval: int  # seconds

def load_config(config_path: str = "config.yaml") -> Config:
    with open(config_path, "r") as file:
        data = yaml.safe_load(file)
    
    plcs = []
    for plc_data in data["plcs"]:
        plc_config = PLCConfig(
            name=plc_data["name"],
            url=plc_data["url"],
            nodes=plc_data.get("nodes", {}),
            auto_discover=plc_data.get("auto_discover", False),
            discover_depth=plc_data.get("discover_depth", 3),
            discover_throttle_ms=plc_data.get("discover_throttle_ms", 100),
            discover_paths=plc_data.get("discover_paths", None),
            discover_cache_time=plc_data.get("discover_cache_time", 0),
            discovery_types=plc_data.get("discovery_types", None)
        )
        plcs.append(plc_config)
    
    db = DBConfig(
        dbname=data["database"]["dbname"],
        user=data["database"]["user"],
        password=data["database"]["password"],
        host=data["database"]["host"],
        port=data["database"]["port"],
    )
    
    return Config(
        plcs=plcs,
        database=db,
        poll_interval=data.get("poll_interval", 5)
    )