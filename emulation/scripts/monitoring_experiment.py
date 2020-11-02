import requests
import datetime
import docker
from influxdb import InfluxDBClient

# experiment parameters
experiment_table = "monitoringEval"
topology = "1Crd10Wrks"
system = "NES"
version = "1"
noIterations = 200

def readDockerStats(containerIters, influxTable, topology, system, version, runtime, iteration):
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
                "version": version
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


print("Executing monitoring request experiment")

# execute experiment
msrmnt_batch = []
prom_url = 'http://localhost:8081/v1/nes/monitoring/metrics/prometheus'
nes_url = 'http://localhost:8081/v1/nes/monitoring/metrics/'
docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')

containers = filter(lambda c: c.name.startswith(("mn.crd", "mn.w")), docker_client.containers.list())
containerIters = [(c.name, c.stats(decode=True)) for c in containers]

for i in range(1, noIterations + 1):
    print("Experiment run " + str(i))
    start = datetime.datetime.now()
    if system == "NES":
        response = requests.get(nes_url)
    elif system == "PROMETHEUS":
        response = requests.get(prom_url)
    else:
        raise RuntimeError("Unkown system " + system)

    end = datetime.datetime.now()
    delta = (end - start)
    runtimeInMs = int(delta.total_seconds() * 1000)  # milliseconds

    if (response.status_code == 200):
        # print(response.json())
        msrmnt_batch.extend(
            readDockerStats(containerIters, experiment_table, topology, system, version, runtimeInMs, i))
        # sleep(0.5)
    else:
        raise RuntimeError("Response with status code " + str(response.status_code))

# influx operations
print("Experiment finished! Writing to Influx..")
influx_client = InfluxDBClient(host='localhost', port=8086)
influx_client.switch_database('mydb')

print(msrmnt_batch)
res = influx_client.write_points(msrmnt_batch)
print(res)

print("Finished!")

# TODO Test prometheus that is not working
