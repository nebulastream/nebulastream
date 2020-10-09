import requests
import datetime
import docker
from influxdb import InfluxDBClient


def readDockerStats(containerIters, influxTable, experimentDesc, runtime, iteration):
    msrmnts = []
    for cName, cIter in containerIters:
        msrmt = next(cIter)
        # print(msrmt) #all metrics that can be collected
        measurementDict = {
            "measurement": influxTable,
            "tags": {
                "cntName": str(cName),
                "description": experimentDesc
            },
            "time": msrmt["read"],
            "fields": {
                "cpuTotalUsage": msrmt["cpu_stats"]["cpu_usage"]["total_usage"],
                "memoryUsage": msrmt["memory_stats"]["usage"],
                "rxBytes": msrmt["networks"]["eth0"]["rx_bytes"],
                "rxPackets": msrmt["networks"]["eth0"]["rx_packets"],
                "txBytes": msrmt["networks"]["eth0"]["tx_bytes"],
                "txPackets": msrmt["networks"]["eth0"]["tx_packets"],
                "runtime(ms)": runtime,
                "iteration": iteration
            }
        }
        print(measurementDict)
        msrmnts.append(measurementDict)
    return msrmnts


print("Executing monitoring request experiment")

# setup operations
prom_url = 'http://localhost:8081/v1/nes/monitoring/metrics/'
docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
# experiment parameters
experiment_table = "monitoringEval"
experiment_description = "1Crd2WrksNesMetricsV1"
noIterations = 300

# execute experiment
msrmnt_batch = []
containers = filter(lambda c: c.name in ["mn.crd"], docker_client.containers.list())
containerIters = [(c.name, c.stats(decode=True)) for c in containers]

for i in range(1, noIterations + 1):
    print("Experiment run " + str(i))
    start = datetime.datetime.now()
    response = requests.get(prom_url)
    end = datetime.datetime.now()
    delta = (end-start)
    runtimeInMs = int(delta.total_seconds() * 1000)  # milliseconds

    if (response.status_code == 200):
        # print(response.json())
        msrmnt_batch.extend(readDockerStats(containerIters, experiment_table, experiment_description, runtimeInMs, i))
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
