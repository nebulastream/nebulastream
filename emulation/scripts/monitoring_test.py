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
from time import sleep

setLogLevel('info')

net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

info('*** Adding docker containers\n')
crd = net.addDocker('crd', ip='10.15.16.3',
                    dimage="nebulastream/nes-executable-image",
                    ports=[8081, 12346, 4000, 4001, 4002],
                    port_bindings={8081: 8081, 12346: 12346, 4000: 4000, 4001: 4001, 4002: 4002},
                    dcmd='/opt/local/nebula-stream/nesCoordinator --serverIp=0.0.0.0')

#TODO: build params will be addressed by issue 1045
w1 = net.addDocker('w1', ip='10.15.16.4',
                       dimage="nes_prometheus",
                       build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                     "path": "/home/xenofon/git/nebulastream/emulation/images"},
                       ports=[3000, 3001, 9100],
                       port_bindings={3007: 3000, 3008: 3001, 9101: 9100})

#TODO: build params will be addressed by issue 1045
w2Prom = net.addDocker('w2Prom', ip='10.15.16.5',
                       dimage="nes_prometheus",
                       build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                     "path": "/home/xenofon/git/nebulastream/emulation/images"},
                       ports=[3000, 3001, 9100],
                       port_bindings={3005: 3000, 3006: 3001, 9100: 9100})

info('*** Adding switches\n')
sw1 = net.addSwitch('sw1')

info('*** Creating links\n')
net.addLink(crd, sw1, cls=TCLink)
net.addLink(w1, sw1, cls=TCLink)
net.addLink(w2Prom, sw1, cls=TCLink)

w1.cmd('/entrypoint-prom.sh /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=10.15.16.4 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')
w2Prom.cmd('/entrypoint-prom.sh /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=10.15.16.5 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')

info('*** Starting network\n')
net.start()

info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()
