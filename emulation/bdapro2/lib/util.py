import mininet.node
import requests
from attr import dataclass

NES_BASE_URL = 'http://localhost:8081/v1/nes'


@dataclass
class NodeMessage:
    node: mininet.node.Docker
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
    resp = requests.post(topology_add_parent_url, {"childId": str(child_id), "parentId": str(parent_id)})
    if resp.status_code != 200:
        raise RuntimeError(f"Could not add node {parent_id} as parent for {child_id}:  {str(resp.text)}")
