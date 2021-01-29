from ipaddress import IPv4Network
from logging import info
from signal import signal, SIGINT
from typing import Iterable, List

# noinspection PyUnresolvedReferences
import mininet.node  # https://github.com/mininet/mininet/issues/546 for why import is needed.
from attr import dataclass
from mininet.link import TCLink
from mininet.net import Containernet

from lib.topology import Topology
from lib.util import NodeCmd, add_parent_topology, NodeType, get_node_ips

COORDINATOR = '/opt/local/nebula-stream/nesCoordinator'
WORKER = '/opt/local/nebula-stream/nesWorker'

ips: Iterable[str] = (str(addr) for addr in IPv4Network('10.220.1.0/24').hosts())


@dataclass
class Config:
    coordinator_port: int = 4000
    coordinator_ip: str = ""
    total_workers: int = 5
    workers_producing: int = 3
    physical_stream_prefix: str = ""
    producer_options: str = f"--sourceType=YSBSource " \
                            f"--numberOfBuffersToProduce=1000 --numberOfTuplesToProducePerBuffer=10 " \
                            f"--sourceFrequency=1 --logicalStreamName=ysb"


def flat_topology(config: Config):
    def f(net: Containernet) -> (Containernet, int, List[NodeCmd]):
        config.coordinator_ip = next(ips)
        nodes: List[NodeCmd] = []
        crd = net.addDocker('crd', ip=config.coordinator_ip, dimage="bdapro/query-merging", ports=[8081, 4000],
                            port_bindings={8081: 8081, 4000: 4000}, volumes=["/tmp/logs:/logs"])
        nodes.append(NodeCmd(crd, NodeType.Coordinator, config.coordinator_ip,
                             f"{COORDINATOR} --restIp=0.0.0.0 --coordinatorIp={config.coordinator_ip} --logLevel"
                             f"=LOG_DEBUG"))
        for i in range(0, config.workers_producing):
            worker_ip = next(ips)
            worker = net.addDocker(f"wp{i}", ip=worker_ip,
                                   dimage="bdapro/query-merging",
                                   volumes=["/tmp/logs:/logs"]
                                   )

            worker_cmd = f"{WORKER} --logLevel=LOG_DEBUG --coordinatorPort={config.coordinator_port}" \
                         f" --coordinatorIp={config.coordinator_ip} --localWorkerIp={worker_ip} " \
                         f"--physicalStreamName={config.physical_stream_prefix}-{i} {config.producer_options} "
            nodes.append(NodeCmd(worker, NodeType.Sensor, worker_ip, worker_cmd))

        for i in range(0, config.total_workers - config.workers_producing):
            worker_ip = next(ips)
            worker = net.addDocker(f"wb{i}", ip=worker_ip,
                                   dimage="bdapro/query-merging",
                                   volumes=["/tmp/logs:/logs"]
                                   )
            worker_cmd = f"{WORKER} --logLevel=LOG_DEBUG --coordinatorPort={config.coordinator_port}" \
                         f" --coordinatorIp={config.coordinator_ip} --localWorkerIp={worker_ip}"
            nodes.append(NodeCmd(worker, NodeType.Worker, worker_ip, worker_cmd))

        info('*** Adding switches\n')
        sw1 = net.addSwitch('sw1')
        info('*** Creating links\n')
        net.addLink(crd, sw1, cls=TCLink)
        for worker in nodes:
            net.addLink(worker.node, sw1, cls=TCLink)
        return net, config.total_workers, nodes

    return f


def handler_fn(topology: Topology):
    def f(signal_received, frame):
        # Handle any cleanup here
        print('SIGINT or CTRL-C detected. Exiting gracefully')
        topology.stop_emulation()
        exit(0)

    return f


def construct_hierarchy(topology: Topology):
    producers = [node.ip for node in topology.nodes if node.node_type == NodeType.Sensor]
    workers = [node.ip for node in topology.nodes if node.node_type == NodeType.Worker]
    node_ips = get_node_ips()
    for i in range(0, min(len(producers), len(workers))):
        add_parent_topology(node_ips.get(producers[i]), node_ips.get(workers[i]))


def main(hierarchy: bool = False):
    topology = Topology()
    topology_config = Config()
    topology_config.physical_stream_prefix = "ysb"
    try:
        topology.create_topology(flat_topology(topology_config))
        topology.start_emulation()
        topology.wait_until_topology_is_complete(60)
        signal(SIGINT, handler_fn(topology))
        if hierarchy:
            print("Constructing Hierarchy")
            construct_hierarchy(topology)
        print("Emulation Running. Press CTRL-C to exit.", flush=True)
        while True:
            # Do nothing and hog CPU forever until SIGINT received.
            pass
    finally:
        print("Destroying topology")
        topology.stop_emulation()


if __name__ == '__main__':
    main(False)
