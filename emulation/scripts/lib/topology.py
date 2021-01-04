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

from mininet.net import Containernet
from mininet.node import Controller
from mininet.link import TCLink
from mininet.log import info

from lib.util import MonitoringType
from enum import Enum


class LogLevel(Enum):
    NONE = "LOG_NONE"
    DEBUG = "LOG_DEBUG"
    INFO = "LOG_INFO"

class Topology:
    def __init__(self, nes_dir, influx_storage, nes_log_level, number_workers_producing, number_workers_not_producing,
                 num_tuples, num_buffers, monitoring_type):
        self.nes_dir = nes_dir
        self.influx_storage = influx_storage
        self.nes_log_level = nes_log_level
        self.number_workers_producing = number_workers_producing
        self.number_workers_not_producing = number_workers_not_producing
        self.num_tuples = num_tuples
        self.num_buffers = num_buffers
        # check monitoring type
        if monitoring_type in [MonitoringType.DISABLED, MonitoringType.NEMO_PULL, MonitoringType.PROMETHEUS]:
            self.monitoring_type = monitoring_type
        else:
            raise RuntimeError("Monitoring type " + monitoring_type + " is not valid")

    def create_topology(self):
        net = Containernet(controller=Controller)
        info('*** Adding controller\n')
        net.addController('c0')

        if not self.nes_dir:
            raise RuntimeError("Please specify a nes directory like /git/nebulastream/")

        if self.influx_storage:
            influxdb = net.addDocker('influxdb', dimage="influxdb:1.8.3",
                                     ports=[8086],
                                     port_bindings={8086: 8086},
                                     volumes=[
                                         self.nes_dir + "emulation/influxdb/influxdb.conf:/etc/influxdb/influxdb.conf:ro",
                                         self.influx_storage + ":/var/lib/influxdb"],
                                     dcmd='influxd -config /etc/influxdb/influxdb.conf')

        info('*** Adding docker containers\n')

        crd = net.addDocker('crd', ip='10.15.16.3',
                            dimage="nes_prometheus",
                            build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                          "path": self.nes_dir + "emulation/images"},
                            ports=[8081, 12346, 4000, 4001, 4002, 9100],
                            port_bindings={8081: 8081, 12346: 12346, 4000: 4000, 4001: 4001, 4002: 4002, 9100: 9100})

        workers = []
        for i in range(0, self.number_workers_producing + self.number_workers_not_producing):
            ip = '10.15.16.' + str(4 + i)
            w = net.addDocker('w' + str(i), ip=ip,
                              dimage="nes_prometheus",
                              build_params={"dockerfile": "Dockerfile-NES-Prometheus",
                                            "path": self.nes_dir + "emulation/images"},
                              ports=[3000, 3001, 9100],
                              port_bindings={3007: 3000, 3008: 3001, 9101: 9100})

            if i < self.number_workers_producing:
                cmd = '/entrypoint-prom.sh ' + self.monitoring_type.value + ' wrk /opt/local/nebula-stream/nesWorker --logLevel=' + self.nes_log_level.value \
                      + ' --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=' + ip \
                      + ' --sourceType=YSBSource --numberOfBuffersToProduce=' + str(self.num_buffers) \
                      + ' --numberOfTuplesToProducePerBuffer=' + str(self.num_tuples) \
                      + ' --sourceFrequency=1 --physicalStreamName=ysb' + str(i) + ' --logicalStreamName=ysb'
            else:
                cmd = '/entrypoint-prom.sh wrk /opt/local/nebula-stream/nesWorker --logLevel=' + self.nes_log_level.value + ' --coordinatorPort=4000 --coordinatorIp=10.15.16.3 --localWorkerIp=' + ip

            print('Worker ' + str(i) + ": " + cmd)
            workers.append((w, cmd))

        info('*** Adding switches\n')
        sw1 = net.addSwitch('sw1')

        info('*** Creating links\n')
        net.addLink(crd, sw1, cls=TCLink)
        for w in workers:
            net.addLink(w[0], sw1, cls=TCLink)

        crd.cmd('/entrypoint-prom.sh ' + self.monitoring_type.value + ' crd /opt/local/nebula-stream/nesCoordinator --restIp=0.0.0.0 --coordinatorIp=10.15.16.3 --logLevel=' + self.nes_log_level.value)
        for w in workers:
            w[0].cmd(w[1])

        return net
