#!/usr/bin/python

from mininet.net import Containernet
from mininet.node import UserSwitch, Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel

"""
Create a topology with a UserSwitch, so more than 50 nodes
can connect at the same time.
"""
setLogLevel('info')
net = Containernet(controller=Controller, switch=UserSwitch)

info('*** Adding controller\n')
net.addController('c0')
crd = net.addDocker('crd', ip='10.1.1.1', dimage='ubuntu:trusty')

info('*** Adding Switches\n')
sw1=net.addSwitch('sw1')

info('*** Adding Nodes\n')
node_num = 60
nodes = [net.addDocker('w' + str(i+1), ip='10.0.0.'+str(i+1), dimage='ubuntu:trusty') for i in range(2, node_num)]
for node in nodes:
	net.addLink(node, sw1)


info('*** Starting net\n')
net.start()

info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()
