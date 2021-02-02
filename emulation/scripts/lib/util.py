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

import requests
from influxdb import InfluxDBClient
from enum import Enum


class MonitoringType(Enum):
    DISABLED = "disabled"
    NEMO_PULL = "nemo_pull"
    PROMETHEUS = "prometheus"


def readDockerStats(containerIters, influxTable, version, runtime, iteration, description,
                    monitoring, monitoring_frequency, no_worker_producing, no_worker_not_producing, no_coordinator):
    msrmnts = []
    for cName, cIter in containerIters:
        msrmt = next(cIter)

        if cName.startswith("mn.w"):
            nes_type = "worker"
        else:
            nes_type = "coordinator"

        # print(msrmt) #all metrics that can be collected
        measurementDict = {
            "measurement": influxTable,
            "tags": {
                "container": str(cName),
                "type": nes_type,
                "version": version,
                "description": description,
                "monitoring_type": str(monitoring),
                "monitoring_frequency": monitoring_frequency,
                "no_worker_producing": no_worker_producing,
                "no_worker_not_producing": no_worker_not_producing,
                "no_worker": no_worker_producing + no_worker_not_producing,
                "no_coordinator": no_coordinator
            },
            "time": msrmt["read"],
            "fields": {
                "cpuTotalUsage": msrmt["cpu_stats"]["cpu_usage"]["total_usage"],
                "memoryUsage": msrmt["memory_stats"]["usage"],
                "rxBytes": msrmt["networks"]["eth0"]["rx_bytes"],
                "rxPackets": msrmt["networks"]["eth0"]["rx_packets"],
                "txBytes": msrmt["networks"]["eth0"]["tx_bytes"],
                "txPackets": msrmt["networks"]["eth0"]["tx_packets"],
                "runtime_ms": runtime,
                "iteration": iteration
            }
        }
        print(measurementDict)
        msrmnts.append(measurementDict)
    return msrmnts


def make_request(request_type):
    if request_type == "ysb":
        nes_url = 'http://localhost:8081/v1/nes/query/execute-query'
        data = '''{
        \"userQuery\" : \"Query::from(\\\"ysb\\\").filter(Attribute(\\\"event_type\\\") < 3).windowByKey(Attribute(\\\"campaign_id\\\"), TumblingWindow::of(EventTime(Attribute(\\\"current_ms\\\")), Milliseconds(100)), Sum(Attribute(\\\"user_id\\\"))).sink(FileSinkDescriptor::create(\\\"ysbOut.csv\\\",\\\"CSV_FORMAT\\\",\\\"APPEND\\\"));\",
        \"strategyName\" : \"BottomUp\"
        }'''
        return requests.post(nes_url, data=data)
    elif request_type == "monitoring_prometheus":
        nes_url = 'http://localhost:8081/v1/nes/monitoring/metrics/prometheus'
        return requests.get(nes_url)
    elif request_type == "monitoring_nes":
        nes_url = 'http://localhost:8081/v1/nes/monitoring/metrics/'
        return requests.get(nes_url)
    else:
        raise RuntimeError("Unkown request " + request_type)


def request_monitoring_data(type):
    print("Executing monitoring call on topology with monitoring " + type.value)
    resp = None
    if type == MonitoringType.PROMETHEUS:
        resp = make_request("monitoring_prometheus")
    elif type == MonitoringType.NEMO_PULL or type == MonitoringType.DISABLED:
        resp = make_request("monitoring_nes")
    else:
        raise RuntimeError("Monitoring request with type " + type.value + " not supported")

    if resp and resp.status_code == 200:
        return resp
    else:
        raise RuntimeError("Response with status code " + str(resp.status_code))


def store_to_influx(database, msrmnt_batch):
    influx_client = InfluxDBClient(host='localhost', port=8086)

    try:
        influx_client.create_database(database)
    except:
        print('Database ' + database + ' already exists')

    influx_client.switch_database(database)
    print(msrmnt_batch)
    print("Writing measurement to database " + database)
    res = influx_client.write_points(msrmnt_batch)
    print(res)
