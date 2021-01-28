from ipaddress import IPv4Network
from logging import info
from time import sleep
from typing import Iterable, List

# noinspection PyUnresolvedReferences
import mininet.node  # https://github.com/mininet/mininet/issues/546 for why import is needed.
from attr import dataclass
from mininet.link import TCLink
from mininet.net import Containernet

from lib.topology import Topology
from lib.util import NodeMessage

COORDINATOR = '/opt/local/nebula-stream/nesCoordinator'

ips: Iterable[str] = (str(addr) for addr in IPv4Network('10.220.1.0/24').hosts())


@dataclass
class Config:
    coordinator_port: int = 4000
    coordinator_ip: str = "10.15.16.3"
    n_buffers_to_produce: int = 1000
    n_tuples_per_buffer: int = 200
    total_workers: int = 5
    workers_producing: int = 3


def flat_topology(config: Config):
    def f(net: Containernet) -> (Containernet, int, List[NodeMessage]):
        next(ips)
        config.coordinator_ip = next(ips)
        nodes: List[NodeMessage] = []
        crd = net.addDocker('crd', ip=config.coordinator_ip, dimage="bdapro/query-merging", ports=[8081, 4000],
                            port_bindings={8081: 8081, 4000: 4000}, volumes=["/tmp/logs:/logs"])
        nodes.append(NodeMessage(crd,
                                 f"{COORDINATOR} --restIp=0.0.0.0 --coordinatorIp={config.coordinator_ip} --logLevel=LOG_DEBUG"))
        for i in range(0, config.workers_producing):
            next(ips)
            worker_ip = next(ips)
            worker = net.addDocker('wp' + str(i), ip=worker_ip,
                                   dimage="bdapro/query-merging",
                                   volumes=["/tmp/logs:/logs"]
                                   )
            worker_cmd = f"/opt/local/nebula-stream/nesWorker --logLevel=LOG_DEBUG --coordinatorPort={config.coordinator_port}" \
                         f" --coordinatorIp={config.coordinator_ip} --localWorkerIp={worker_ip} --sourceType=YSBSource " \
                         f"--numberOfBuffersToProduce={config.n_buffers_to_produce} --numberOfTuplesToProducePerBuffer={config.n_buffers_to_produce} " \
                         f"--sourceFrequency=1 --physicalStreamName=ysb1 --logicalStreamName=ysb"
            nodes.append(NodeMessage(worker, worker_cmd))

        for i in range(0, config.total_workers - config.workers_producing):
            next(ips)
            worker_ip = next(ips)
            worker = net.addDocker('wb' + str(i), ip=worker_ip,
                                   dimage="bdapro/query-merging",
                                   volumes=["/tmp/logs:/logs"]
                                   )
            worker_cmd = f"/opt/local/nebula-stream/nesWorker --logLevel=LOG_DEBUG --coordinatorPort={config.coordinator_port}" \
                         f" --coordinatorIp={config.coordinator_ip} --localWorkerIp={worker_ip}"
            nodes.append(NodeMessage(worker, worker_cmd))

        info('*** Adding switches\n')
        sw1 = net.addSwitch('sw1')
        info('*** Creating links\n')
        net.addLink(crd, sw1, cls=TCLink)
        for worker in nodes:
            net.addLink(worker.node, sw1, cls=TCLink)
        return net, config.total_workers, nodes

    return f


def main():
    topology = Topology()
    topology.create_topology(flat_topology(Config()))
    topology.start_emulation()
    topology.wait_until_topology_is_complete(60)
    print("Started")
    sleep(1000)
    topology.stop_emulation()


if __name__ == '__main__':
    main()
