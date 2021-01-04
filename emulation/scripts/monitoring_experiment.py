from lib.topology import Topology, LogLevel
from lib.experiment import Experiment
from lib.util import MonitoringType

from mininet.cli import CLI
from mininet.log import info, setLogLevel
from time import sleep

setLogLevel('info')

# repeatable parameters for each experiment
# ---------------------------------------------------------------------
number_workers_producing = [5]
number_workers_not_producing = [0]
monitoring_types = [MonitoringType.DISABLED, MonitoringType.NEMO_PULL, MonitoringType.PROMETHEUS]

# topology parameters
# ---------------------------------------------------------------------
num_tuples = 25
num_buffers = 30

nes_dir = "/home/xenofon/git/nebulastream/"
influx_storage = "/home/xenofon/experiments/influx/"
nes_log_level = LogLevel.NONE

# experiment parameters
# ---------------------------------------------------------------------
version = "2"
iterations = 30
iterations_before_execution = 0
monitoring_frequency = 1
no_coordinators = 1
influx_db = "monitoring"
influx_table = "monitoring_measurement_ysb"
description = str(num_tuples) + "Tup-" + str(num_buffers) + "Buf"


# experiment method definitions
# ---------------------------------------------------------------------
def execute_experiment(_nes_dir, _influx_storage, _nes_log_level, _number_workers_producing,
                       _number_workers_not_producing, _num_tuples, _num_buffers, _monitoring_type, _influx_db,
                       _influx_table, _iterations, _iterations_before_execution, _monitoring_frequency,
                       _no_coordinators, _description, _version, run_cli, sleep_time):
    topology = Topology(_nes_dir, _influx_storage, _nes_log_level, _number_workers_producing,
                        _number_workers_not_producing, _num_tuples, _num_buffers, _monitoring_type)
    net = topology.create_topology()

    info('*** Starting network\n')
    net.start()

    info('*** Sleeping ' + str(sleep_time) + '\n')
    sleep(sleep_time)

    info('*** Executing experiment with following parameters producers=' + str(
        worker_producing) + '; non_producers=' + str(worker_not_producing) + '; monitoring_type=' + mt.value + '\n')
    experiment = Experiment(_number_workers_producing, _number_workers_not_producing, _influx_db, _influx_table,
                            _iterations,
                            _iterations_before_execution, _monitoring_frequency, _no_coordinators, _monitoring_type,
                            _description, _version)
    experiment.start()

    if run_cli:
        info('*** Running CLI\n')
        CLI(net)
    else:
        sleep(sleep_time)

    info('*** Stopping network')
    net.stop()


# experiment the executable part
# ---------------------------------------------------------------------
for worker_producing in number_workers_producing:
    for worker_not_producing in number_workers_not_producing:
        for mt in monitoring_types:
            info('*** Executing experiment with following parameters producers=' + str(
                worker_producing) + '; non_producers=' + str(worker_not_producing) + '; monitoring_type=' + mt.value + '\n')

            execute_experiment(nes_dir, influx_storage, nes_log_level, worker_producing,
                               worker_not_producing, num_tuples, num_buffers, mt, influx_db,
                               influx_table, iterations, iterations_before_execution, monitoring_frequency,
                               no_coordinators, description, version, False, 10)
            sleep(10)
