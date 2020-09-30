#!/usr/bin/python

from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
setLogLevel('info')

net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

info('*** Adding docker containers\n')
crd = net.addDocker('crd', ip='10.1.1.1',
                            dimage="nebulastream/nes-executable-image",
                            ports=[8081, 12346, 4000, 4001, 4002],
                            port_bindings={8081:8081, 12346:12346, 4000:4000, 4001:4001, 4002:4002},
                            dcmd='/opt/local/nebula-stream/nesCoordinator --serverIp=0.0.0.0')

w1 = net.addDocker('w1', ip='10.1.1.2',
                            dimage="nebulastream/nes-executable-image",
                            ports=[3000, 3001],
                            port_bindings={3000:3000, 3001:3001})

w2 = net.addDocker('w2', ip='10.1.1.3',
                            dimage="nebulastream/nes-executable-image",
                            ports=[3000, 3001],
                            port_bindings={3002:3000, 3003:3001})

w3 = net.addDocker('w3', ip='10.1.1.4',
                            dimage="nebulastream/nes-executable-image",
                            ports=[3000, 3001],
                            port_bindings={3004:3000, 3005:3001})


info('*** Adding switches\n')
sw1 = net.addSwitch('sw1')

info('*** Creating links\n')
net.addLink(crd, sw1, cls=TCLink)
net.addLink(w1, sw1, cls=TCLink)
net.addLink(w2, sw1, cls=TCLink)
net.addLink(w3, sw1, cls=TCLink)

w1.cmd('/entrypoint-containernet.sh /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.1.1.1 --localWorkerIp=10.1.1.2 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')
w2.cmd('/entrypoint-containernet.sh /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.1.1.1 --localWorkerIp=10.1.1.3 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')
w3.cmd('/entrypoint-containernet.sh /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.1.1.1 --localWorkerIp=10.1.1.4 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')

info('*** Starting network\n')
net.start()

info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()