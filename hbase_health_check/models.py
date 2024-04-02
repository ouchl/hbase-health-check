from dataclasses import dataclass
from typing import List, Dict

@dataclass
class Region:
    name: str
    table_name: str
    store_file_size_gb: float = 0


@dataclass
class Table:
    name: str
    total_requests: int = 0
    column_families: int = 0


@dataclass
class RegionServer:
    instance_id: str
    instance_dns_name: str
    region_num: int
    total_requests: int
    tables: List[Table]
    regions: List[Region]
    store_files: int
    l1_hit_ratio: float
    l2_hit_ratio: float
    split_num: int
    gc_num: int
    gc_time: str


@dataclass
class HBaseCluster:
    cluster_id: str
    # cluster_description: dict
    master_dns_name: str
    configurations: List[dict]
    region_servers: List[RegionServer]
    live_region_servers: List[str]

