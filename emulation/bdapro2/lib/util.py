from enum import Enum
from typing import Dict

import mininet.node
import requests
from attr import dataclass

NES_BASE_URL = 'http://localhost:8081/v1/nes'


class NodeType(Enum):
    Coordinator = "coordinator"
    Sensor = "sensor"
    Worker = "worker"


@dataclass
class NodeCmd:
    node: mininet.node.Docker
    node_type: NodeType
    ip: str
    cmd: str


def request_monitoring_data():
    print("Executing monitoring call on topology")
    monitoring_url = f"{NES_BASE_URL}/monitoring/metrics/"
    resp = requests.get(monitoring_url)
    if resp and resp.status_code == 200:
        return resp
    else:
        raise RuntimeError("Response with status code " + str(resp.status_code))


def add_parent_topology(child_id: int, parent_id: int):
    print(f"Adding {parent_id} as parent for {child_id}")
    topology_add_parent_url = f"{NES_BASE_URL}/topology/addParent"
    resp = requests.post(topology_add_parent_url,
                         json={"childId": str(child_id), "parentId": str(parent_id)})
    if resp.status_code != 200:
        raise RuntimeError(
            f"Could not add node {parent_id} as parent for {child_id}: Response Code: {resp.status_code}  {resp.text}")


def remove_parent_topology(child_id: int, parent_id: int):
    print(f"Remove {parent_id} as parent for {child_id}")
    topology_add_parent_url = f"{NES_BASE_URL}/topology/removeParent"
    resp = requests.post(topology_add_parent_url,
                         json={"childId": str(child_id), "parentId": str(parent_id)})
    if resp.status_code != 200:
        raise RuntimeError(
            f"Could not remove node {parent_id} as parent for {child_id}: Response Code: {resp.status_code}  {resp.text}")


def get_node_ips() -> Dict[str, int]:
    """
    Fetch IP address to node ID mapping.
    :return:  Dict of IP Address to Node ID mapping
    """
    topology = _get_physical_topology()
    nodes = topology["nodes"]
    res: Dict[str, int] = {}
    for node in nodes:
        res[node["ip_address"]] = node["id"]
    return res


def _get_physical_topology():
    print(f"Executing get topology")
    topology_url = f"{NES_BASE_URL}/topology"
    resp = requests.get(topology_url)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Error fetching topology from Coordinator: Response Code: {resp.status_code}  {resp.text}")
    return resp.json()
