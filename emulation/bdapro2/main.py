import logging
from ipaddress import IPv4Network
from logging import info
from pathlib import Path
from signal import signal, SIGINT, SIGTERM
from typing import Iterable, List, Set, Dict, Optional

# noinspection PyUnresolvedReferences
import mininet.node  # https://github.com/mininet/mininet/issues/546 for why import is needed.
from dataclasses import dataclass
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Containernet
from yamldataclassconfig.config import YamlDataClassConfig

from lib.topology import Topology
from lib.util import NodeCmd, add_parent_topology, NodeType, get_node_ips, remove_parent_topology

COORDINATOR = "/opt/local/nebula-stream/nesCoordinator"
WORKER = "/opt/local/nebula-stream/nesWorker"
SENSOR_WORKER_PREFIX = "S"
WORKER_PREFIX = "W"

ips: Iterable[str] = (str(addr) for addr in IPv4Network('10.220.1.0/24').hosts())


@dataclass
class Config(YamlDataClassConfig):
    coordinator_port: int = 4000
    coordinator_ip: str = ""
    total_workers: int = None
    workers_producing: int = None
    physical_stream_prefix: str = None
    producer_options: str = None
    hierarchy: bool = None
    log_level: str = "debug"
    hierarchy_mapping: Optional[Dict[str, Set[str]]] = None


def flat_topology(config: Config):
    def f(net: Containernet) -> (Containernet, int, List[NodeCmd]):
        config.coordinator_ip = next(ips)
        nodes: List[NodeCmd] = []
        crd = net.addDocker('crd', ip=config.coordinator_ip, dimage="bdapro/query-merging", ports=[8081, 4000],
                            port_bindings={8081: 8081, 4000: 4000}, volumes=["/tmp/logs:/logs"], cpuset_cpus="4")
        nodes.append(NodeCmd(crd, NodeType.Coordinator, config.coordinator_ip,
                             f"{COORDINATOR} --restIp=0.0.0.0 --coordinatorIp={config.coordinator_ip} --numberOfSlots"
                             f"=8 --logLevel=LOG_DEBUG --enableQueryMerging=true"))
        for i in range(0, config.workers_producing):
            worker_ip = next(ips)
            worker = net.addDocker(f"{SENSOR_WORKER_PREFIX}-{i}", ip=worker_ip,
                                   dimage="bdapro/query-merging",
                                   volumes=["/tmp/logs:/logs"],
                                   cpuset_cpus="1",
                                   )
            worker_cmd = f"{WORKER} --logLevel=LOG_DEBUG --coordinatorPort={config.coordinator_port}" \
                         f" --coordinatorIp={config.coordinator_ip} --localWorkerIp={worker_ip} " \
                         f"--numberOfSlots=8 --physicalStreamName={config.physical_stream_prefix}-{i} {config.producer_options} "
            nodes.append(NodeCmd(worker, NodeType.Sensor, worker_ip, worker_cmd))

        for i in range(0, config.total_workers - config.workers_producing):
            worker_ip = next(ips)
            worker = net.addDocker(f"{WORKER_PREFIX}-{i}", ip=worker_ip,
                                   dimage="bdapro/query-merging",
                                   volumes=["/tmp/logs:/logs"],
                                   cpuset_cpus="1"
                                   )
            worker_cmd = f"{WORKER} --logLevel=LOG_DEBUG --coordinatorPort={config.coordinator_port}" \
                         f" --coordinatorIp={config.coordinator_ip} --localWorkerIp={worker_ip} --numberOfSlots=8"
            nodes.append(NodeCmd(worker, NodeType.Worker, worker_ip, worker_cmd))

        info('*** Adding switches\n')
        sw1 = net.addSwitch('sw1')
        info('*** Creating links\n')
        net.addLink(crd, sw1, cls=TCLink)
        for worker in nodes:
            net.addLink(worker.node, sw1, cls=TCLink)
        return net, config.total_workers, nodes

    return f


class GracefulKiller:

    def __init__(self, topology: Topology):
        self._topology = topology
        signal(SIGINT, self.exit_gracefully)
        signal(SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print('SIGINT or CTRL-C detected. Exiting gracefully', flush=True)
        self._topology.stop_emulation()
        exit(0)


def construct_hierarchy(topology: Topology, config: Config):
    node_ips = get_node_ips()
    hierarchy_mapping = config.hierarchy_mapping
    if hierarchy_mapping is None:
        default_hierarchy(topology, node_ips)
    else:
        coordinator_worker_id = node_ips.get(config.coordinator_ip)
        node_name_ip = dict()
        for node in topology.nodes:
            node_name_ip[node.node.name] = node.ip
        for parent in hierarchy_mapping.keys():
            for child in hierarchy_mapping[parent]:
                child_ip = node_name_ip[child]
                parent_ip = node_name_ip[parent]
                print(f"construct_hierarchy: Adding {parent} as parent for {child}")
                child_id = node_ips.get(child_ip)
                parent_id = node_ips.get(parent_ip)
                add_parent_topology(child_id, parent_id)
                remove_parent_topology(child_id, coordinator_worker_id)


def default_hierarchy(topology: Topology, node_ips: Dict[str, int]):
    producers = [node.ip for node in topology.nodes if node.node_type == NodeType.Sensor]
    workers = [node.ip for node in topology.nodes if node.node_type == NodeType.Worker]
    for i in range(0, min(len(producers), len(workers))):
        add_parent_topology(node_ips.get(producers[i]), node_ips.get(workers[i]))


def main(config: Config):
    topology = Topology()
    try:
        topology.create_topology(flat_topology(CONFIG))
        topology.start_emulation()
        topology.wait_until_topology_is_complete(60)
        GracefulKiller(topology)
        if config.hierarchy:
            print("Constructing Hierarchy")
            construct_hierarchy(topology, config)
        print("Emulation Running. Press CTRL-C to exit.", flush=True)
        while True:
            # Do nothing and hog CPU forever until SIGINT received.
            pass
    finally:
        print("Destroying topology")
        topology.stop_emulation()


if __name__ == '__main__':
    CONFIG: Config = Config()
    CONFIG.load(f"{Path(__file__).parent}/config.yaml", path_is_absolute=True)
    setLogLevel(CONFIG.log_level)
    logging.basicConfig(level=CONFIG.log_level.upper())
    assert CONFIG.total_workers >= CONFIG.workers_producing, "Total workers should be equal to or more than workers " \
                                                             "producing "
    print(f"Running with configuration: {CONFIG}", flush=True)
    main(config=CONFIG)
