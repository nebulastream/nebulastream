from time import sleep
from typing import Callable, Tuple, List

from mininet.log import info
from mininet.net import Containernet
from mininet.node import Controller

from lib.util import request_monitoring_data, NodeCmd


class Topology:
    def __init__(self):
        self.net = None
        self.topology_size: int = 0
        self.nodes: List[NodeCmd] = []
        self._stopped: bool = True

    def create_topology(self, topology: Callable[[Containernet], Tuple[Containernet, int, List[NodeCmd]]]):
        net = Containernet(controller=Controller)
        info('*** Adding controller\n')
        net.addController('c0')
        self.net, self.topology_size, self.nodes = topology(net)

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

            if current_size == len(self.nodes):
                print(f"Completed: Received monitoring data from coordinator for nodes {current_size}")
                complete = True
                break
            else:
                print(
                    f"Waiting: Received monitoring data from coordinator for nodes {current_size} but expecting {len(self.nodes)}")
                sleep(5)
        return complete

    def start_emulation(self):
        self.net.start()
        self._stopped = False
        for worker in self.nodes:
            # Redirect output or else containernet waits for it, preventing shutdown.
            worker.node.cmdPrint(f"/entrypoint-containernet.sh {worker.cmd} > /nes-runtime.log 2>&1 &")
            print(f"Executed CMD {worker.cmd} and on Worker {worker.node.name}")

    def stop_emulation(self):
        if not self._stopped:
            print("Attempting to stop emulation.", flush=True)
            print("Starting network stop.", flush=True)
            self.net.stop()
            print("Network stopped.", flush=True)
            self._stopped = True
