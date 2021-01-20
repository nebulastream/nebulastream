#!/usr/bin/env python
# Copyright (C) 2021 by the NebulaStream project (https://nebula.stream)

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
from mininet.node import UserSwitch, Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel

"""
Create a topology with a UserSwitch, so more than 50 nodes
can connect at the same time. The Containernet ctor uses
a switch of type UserSwitch.
"""
setLogLevel('info')
# use of new switch as parameter to the ctor here
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
