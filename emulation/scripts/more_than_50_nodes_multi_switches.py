#!/usr/bin/python

from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel

"""
Use multiple switchs for connecting more than 50
nodes at the same time in total. Each switch has
50 nodes assigned to it at all times.
"""
setLogLevel('info')
net = Containernet(controller=Controller)


info('*** Adding controller\n')
net.addController('c0')
crd = net.addDocker('crd', ip='10.1.1.1', dimage='ubuntu:trusty')


info('*** Adding Switches\n')
sw1=net.addSwitch('sw1')
sw2=net.addSwitch('sw2')
net.addLink(sw1, sw2, cls=TCLink)

info('*** Adding Nodes\n')
node_string = 'w'
ip_index = 2
base_ip_prefix = '10.1.1.'

for i in range(0,52):
	curr_node_string = node_string + str(i)
	curr_ip = base_ip_prefix + str(ip_index)
	node = net.addDocker(curr_node_string, ip=curr_ip, dimage='ubuntu:trusty')
	ip_index += 1
	net.addLink(node, sw1)

for i in range(53,105):
	curr_node_string = node_string + str(i)
	curr_ip = base_ip_prefix + str(ip_index)
	node = net.addDocker(curr_node_string, ip=curr_ip, dimage='ubuntu:trusty')
	ip_index += 1
	net.addLink(node, sw2)

info('*** Starting net\n')
net.start()

info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()
