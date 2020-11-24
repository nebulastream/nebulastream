#!/usr/bin/env python
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/python

from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel

#params for the topology
number_workers = 2
enable_influx = False

#_________________________________________________________________________________________________________________
setLogLevel('info')

net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

nesDir = "/home/xchatziliadis/git/nebulastream/"

if enable_influx:
    influxdb = net.addDocker('influxdb', dimage="influxdb:1.8.3",
                             ports=[8086],
                             port_bindings={8086: 8086},
                             volumes=[nesDir + "emulation/data/influxdb/influxdb.conf:/etc/influxdb/influxdb.conf:ro",
                                      nesDir + "emulation/data/influxdb:/var/lib/influxdb"],
                             dcmd='influxd -config /etc/influxdb/influxdb.conf')

info('*** Adding docker containers\n')
#crd = net.addDocker('crd', ip='10.15.16.3',
#                    dimage="nebulastream/nes-executable-image:latest",
#                    ports=[8081, 12346, 4000, 4001, 4002],
#                    port_bindings={8081: 8081, 12346: 12346, 4000: 4000, 4001: 4001, 4002: 4002},
#                    dcmd='/opt/local/nebula-stream/nesCoordinator --serverIp=0.0.0.0')

crd = net.addDocker('crd', ip='10.15.16.3',
                       dimage="nes_prometheus",
                       build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                     "path": nesDir + "emulation/images"},
                       ports=[8081, 12346, 4000, 4001, 4002, 9100],
                       port_bindings={8081: 8081, 12346: 12346, 4000: 4000, 4001: 4001, 4002: 4002, 9100: 9100})

workers = []
for i in range(0, number_workers):
    ip = '10.15.16.' + str(4+i)
    w = net.addDocker('w'+str(i), ip=ip,
                           dimage="nes_prometheus",
                           build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                         "path": nesDir + "emulation/images"},
                           ports=[3000, 3001, 9100],
                           port_bindings={3007: 3000, 3008: 3001, 9101: 9100})
    cmd = '/entrypoint-prom.sh wrk /opt/local/nebula-stream/nesWorker --logLevel=LOG_TRACE --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=' + ip + ' --sourceType=YSBSource --numberOfBuffersToProduce=100 --numberOfTuplesToProducePerBuffer=10 --sourceFrequency=1 --physicalStreamName=ysb' + str(i) + ' --logicalStreamName=ysb'
    print (cmd)
    workers.append((w, cmd))

info('*** Adding switches\n')
sw1 = net.addSwitch('sw1')

info('*** Creating links\n')
net.addLink(crd, sw1, cls=TCLink)
for w in workers:
    net.addLink(w[0], sw1, cls=TCLink)

crd.cmd('/entrypoint-prom.sh crd /opt/local/nebula-stream/nesCoordinator --restIp=0.0.0.0 --coordinatorIp=10.15.16.3 --logLevel=LOG_DEBUG')
for w in workers:
    w[0].cmd(w[1])

info('*** Starting network\n')
net.start()

info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()
