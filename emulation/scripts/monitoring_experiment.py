import requests
import datetime
import docker
from influxdb import InfluxDBClient
from time import sleep

# experiment parameters
experiment_table = "experiment_monitoring_ysb"
topology = "1Crd5Wrks"
system = "NES"
noIterations = 25
noIterationsBeforeExecution = 5

description = "10Tup-10Buf"

version = "2"

# types are None, "prometheus", "nes"
types = [None, "prometheus", "nes"]
monitoring_type = types[0]


def readDockerStats(containerIters, influxTable, topology, system, version, runtime, iteration, description, monitoring):
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
                "topology": topology,
                "system": system,
                "version": version,
                "description": description,
                "monitoring": str(monitoring)
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
        \"userQuery\" : \"Query::from(\\\"ysb\\\").filter(Attribute(\\\"event_type\\\") < 3).windowByKey(Attribute(\\\"campaign_id\\\"), TumblingWindow::of(EventTime(Attribute(\\\"current_ms\\\")), Milliseconds(1)), Sum(Attribute(\\\"user_id\\\"))).sink(FileSinkDescriptor::create(\\\"ysbOut.csv\\\",\\\"CSV_FORMAT\\\",\\\"APPEND\\\"));\",
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
    print("Executing monitoring call on " + type)
    resp = None
    if type == "prometheus":
        resp = make_request("monitoring_prometheus")
    elif type == "nes":
        resp = make_request("monitoring_nes")

    if resp and resp.status_code == 200:
        return True
    else:
        raise RuntimeError("Response with status code " + str(resp.status_code))

print("Executing monitoring request experiment")
# execute experiment
msrmnt_batch = []
docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')

containers = filter(lambda c: c.name.startswith(("mn.crd", "mn.w")), docker_client.containers.list())
containerIters = [(c.name, c.stats(decode=True)) for c in containers]

start = datetime.datetime.now()
for i in range(1, noIterationsBeforeExecution):
    print("\nExecuting iteration " + str(i))
    if monitoring_type:
        request_monitoring_data(monitoring_type)

    print("Reading docker stats " + str(i))
    end = datetime.datetime.now()
    delta = (end - start)
    runtimeInMs = int(delta.total_seconds() * 1000)  # milliseconds
    msrmnt_batch.extend(readDockerStats(containerIters, experiment_table, topology, system, version, runtimeInMs, i, description, monitoring_type))
    sleep(1)

print("Executing query")
start = datetime.datetime.now()
response = make_request("ysb")

for i in range(noIterationsBeforeExecution, noIterationsBeforeExecution + noIterations + 1):
    print("\nExecuting iteration " + str(i))

    if monitoring_type:
        request_monitoring_data(monitoring_type)

    print("Reading docker stats " + str(i))
    end = datetime.datetime.now()
    delta = (end - start)
    runtimeInMs = int(delta.total_seconds() * 1000)  # milliseconds

    if (response.status_code == 200):
        # print(response.json())
        msrmnt_batch.extend(readDockerStats(containerIters, experiment_table, topology, system, version, runtimeInMs, i, description, monitoring_type))
        sleep(1)
    else:
        raise RuntimeError("Response with status code " + str(response.status_code))

# influx operations
print("Experiment finished! Writing to Influx..")
influx_client = InfluxDBClient(host='localhost', port=8086)

try:
    influx_client.create_database('monitoring')
except:
    print('Database monitoring already exists')

influx_client.switch_database('monitoring')

print(msrmnt_batch)
res = influx_client.write_points(msrmnt_batch)
print(res)

print("Finished!")

# TODO Test prometheus that is not working
