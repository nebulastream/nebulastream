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
s11 = net.addDocker('s11', ip='10.0.0.251', dimage="local-focal", ports=[3000, 3001])
s12 = net.addDocker('s12', ip='10.0.0.252', dimage="ubuntu:trusty")
s21 = net.addDocker('s21', ip='10.0.1.251', dimage="ubuntu:trusty")
s22 = net.addDocker('s22', ip='10.0.1.252', dimage="ubuntu:trusty")

s31 = net.addDocker('s31', ip='10.0.2.251', dimage="ubuntu:trusty")
s32 = net.addDocker('s32', ip='10.0.2.252', dimage="ubuntu:trusty")
s41 = net.addDocker('s41', ip='10.0.3.251', dimage="ubuntu:trusty")
s42 = net.addDocker('s42', ip='10.0.3.252', dimage="ubuntu:trusty")

s51 = net.addDocker('s51', ip='10.0.4.251', dimage="ubuntu:trusty")
s61 = net.addDocker('s61', ip='10.0.5.251', dimage="ubuntu:trusty")
s71 = net.addDocker('s71', ip='10.0.6.251', dimage="ubuntu:trusty")

info('*** Adding switches\n')
sw1 = net.addSwitch('sw1')
sw2 = net.addSwitch('sw2')
sw3 = net.addSwitch('sw3')
sw4 = net.addSwitch('sw4')
sw5 = net.addSwitch('sw5')
sw6 = net.addSwitch('sw6')
sw7 = net.addSwitch('sw7')

info('*** Creating links\n')
net.addLink(s11, sw1, cls=TCLink)
net.addLink(s12, sw1, cls=TCLink)

net.addLink(s21, sw2, cls=TCLink)
net.addLink(s22, sw2, cls=TCLink)

net.addLink(s31, sw3, cls=TCLink)
net.addLink(s32, sw3, cls=TCLink)

net.addLink(s41, sw4, cls=TCLink)
net.addLink(s42, sw4, cls=TCLink)

net.addLink(s51, sw5, cls=TCLink)
net.addLink(s61, sw6, cls=TCLink)
net.addLink(s71, sw7, cls=TCLink)

net.addLink(sw1, sw5, cls=TCLink)
net.addLink(sw2, sw5, cls=TCLink)

net.addLink(sw3, sw6, cls=TCLink)
net.addLink(sw4, sw6, cls=TCLink)

net.addLink(sw5, sw7, cls=TCLink)
net.addLink(sw6, sw7, cls=TCLink)

info('*** Starting network\n')
net.start()
info('*** Testing connectivity\n')
# net.ping([s11, s21])
# net.ping([s11, s71])
info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()