import mininet.node
import requests
from attr import dataclass


@dataclass
class NodeMessage:
    node: mininet.node.Docker
    cmd: str


def request_monitoring_data():
    print("Executing monitoring call on topology")
    nes_url = 'http://localhost:8081/v1/nes/monitoring/metrics/'
    resp = requests.get(nes_url)
    if resp and resp.status_code == 200:
        return resp
    else:
        raise RuntimeError("Response with status code " + str(resp.status_code))
