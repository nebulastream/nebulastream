from time import sleep
from typing import Callable, Tuple, List

from mininet.cli import CLI
from mininet.log import info
from mininet.net import Containernet
from mininet.node import Controller

from lib.util import request_monitoring_data, NodeMessage


class Topology:
    def __init__(self):
        self.net = None
        self.topology_size: int = 0
        self.workers: List[NodeMessage] = []
        self._stopped: bool = True

    def create_topology(self, topology: Callable[[Containernet], Tuple[Containernet, int, List[NodeMessage]]]):
        net = Containernet(controller=Controller)
        info('*** Adding controller\n')
        net.addController('c0')
        self.net, self.topology_size, self.workers = topology(net)

    def wait_until_topology_is_complete(self, timeout):
        timeout = timeout // 5
        complete = False
        current_size = 0
        print("Checking completion of topology with timeout re-tries " + str(timeout))
        for i in range(0, timeout):
            try:
                print(f"({i}/{timeout} Requesting monitoring data..")
                current_size = len(request_monitoring_data().json())
            except RuntimeError as e:
                print("Failed requesting size of topology: ", e)
            except Exception as e:
                print("Failed to connect to REST service of coordinator: ", e)

            if current_size == len(self.workers):
                print(f"Completed: Received monitoring data from coordinator for nodes {current_size}")
                complete = True
                break
            else:
                print(
                    f"Waiting: Received monitoring data from coordinator for nodes {current_size} but expecting {len(self.workers)}")
                sleep(5)
        return complete

    def _start_cli(self):
        CLI(self.net)

    def start_emulation(self):
        self.net.start()
        self._stopped = True
        for worker in self.workers:
            worker.node.sendCmd(worker.cmd)
            print(f"Executed CMD {worker.cmd} and on Worker {worker.node.name}")

    def stop_emulation(self):
        if not self._stopped:
            self._start_cli()
            self.net.stop()
            self._stopped = True
