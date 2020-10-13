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

nesDir = "/home/xenofon/git/nebulastream/"

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

#TODO: build params will be addressed by issue 1045
w1 = net.addDocker('w1', ip='10.15.16.4',
                       dimage="nes_prometheus",
                       build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                     "path": nesDir + "emulation/images"},
                       ports=[3000, 3001, 9100],
                       port_bindings={3007: 3000, 3008: 3001, 9101: 9100})

#TODO: build params will be addressed by issue 1045
w2 = net.addDocker('w2', ip='10.15.16.5',
                   dimage="nes_prometheus",
                   build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                     "path": nesDir + "emulation/images"},
                   ports=[3000, 3001, 9100],
                   port_bindings={3005: 3000, 3006: 3001, 9102: 9100})

info('*** Adding switches\n')
sw1 = net.addSwitch('sw1')

info('*** Creating links\n')
net.addLink(crd, sw1, cls=TCLink)
net.addLink(w1, sw1, cls=TCLink)
net.addLink(w2, sw1, cls=TCLink)

crd.cmd('/entrypoint-prom.sh crd /opt/local/nebula-stream/nesCoordinator --serverIp=0.0.0.0')
w1.cmd('/entrypoint-prom.sh wrk /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=10.15.16.4 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')
w2.cmd('/entrypoint-prom.sh wrk /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=10.15.16.5 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')

info('*** Starting network\n')
net.start()

info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()
