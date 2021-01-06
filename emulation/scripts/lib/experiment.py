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

from time import sleep
import datetime
import docker

from lib.util import request_monitoring_data, readDockerStats, make_request, store_to_influx
from lib.util import MonitoringType


class Experiment:
    def __init__(self, no_workers_producing, no_workers_not_producing, influx_db, influx_table, noIterations,
                 noIterationsBeforeExecution, monitoring_frequency, no_coordinators, monitoring_type, description,
                 version):
        self.no_workers_producing = no_workers_producing
        self.no_workers_not_producing = no_workers_not_producing
        self.influx_db = influx_db
        self.influx_table = influx_table
        self.noIterations = noIterations
        self.noIterationsBeforeExecution = noIterationsBeforeExecution
        self.monitoring_frequency = monitoring_frequency
        self.no_coordinators = no_coordinators
        self.description = description
        self.version = version

        # check monitoring type
        if monitoring_type in [MonitoringType.DISABLED, MonitoringType.NEMO_PULL, MonitoringType.PROMETHEUS]:
            self.monitoring_type = monitoring_type
        else:
            raise RuntimeError("Monitoring type " + monitoring_type + " is not valid")

    def execute_iterations(self, iterations, container_iter, sleep_duration, with_monitoring):
        msrmnt_batch = []
        start = datetime.datetime.now()
        for i in range(1, iterations):
            print("\nExecuting iteration " + str(i))
            if with_monitoring and (self.monitoring_type != MonitoringType.DISABLED) \
                    and ((i % self.monitoring_frequency) == 0):
                resp = request_monitoring_data(self.monitoring_type)
                # print("Received monitoring data from coordinator " + str(resp.json()))

            print("Reading docker stats " + str(i))
            end = datetime.datetime.now()
            delta = (end - start)
            runtimeInMs = int(delta.total_seconds() * 1000)  # milliseconds

            msrmnt_batch.extend(
                readDockerStats(container_iter, self.influx_table, self.version, runtimeInMs, i, self.description,
                                self.monitoring_type.value, self.monitoring_frequency, self.no_workers_producing,
                                self.no_workers_not_producing, self.no_coordinators))
            sleep(sleep_duration)
        return msrmnt_batch

    def start(self, store_measurements):
        print("Executing monitoring request experiment")
        # execute experiment
        msrmnt_batch = []
        docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')

        containers = filter(lambda c: c.name.startswith(("mn.crd", "mn.w")), docker_client.containers.list())
        container_iter = [(c.name, c.stats(decode=True)) for c in containers]

        # make measurements before request
        if self.noIterationsBeforeExecution > 0:
            print("Making measurements before request")
            res = self.execute_iterations(self.noIterationsBeforeExecution, container_iter, 1, False)
            msrmnt_batch.extend(res)

        # execute query on nes
        print("Executing query")
        response = make_request("ysb")
        if response.status_code != 200:
            raise RuntimeError("Response with status code " + str(response.status_code))
        else:
            print("YSB request successful with status code " + str(response.status_code))

        # make measurements after request
        print("Making measurements after request")
        res = self.execute_iterations(self.noIterationsBeforeExecution + self.noIterations, container_iter, 1, True)
        msrmnt_batch.extend(res)

        # influx operations
        if store_measurements:
            print("Experiment finished! Writing to Influx..")
            store_to_influx(self.influx_db, msrmnt_batch)
        sleep(1)

        print("Finished!")
