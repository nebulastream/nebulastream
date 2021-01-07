from lib.topology import Topology, LogLevel
from lib.experiment import Experiment
from lib.util import MonitoringType

from mininet.log import info, setLogLevel
from time import sleep

setLogLevel('info')

# repeatable parameters for each experiment
# ---------------------------------------------------------------------
number_workers_producing = [5]
number_workers_not_producing = [0]
# MonitoringType.DISABLED, MonitoringType.NEMO_PULL, MonitoringType.PROMETHEUS
monitoring_types = [MonitoringType.NEMO_PULL]
store_measurements = False
run_experiment = False

# topology parameters
# ---------------------------------------------------------------------
num_tuples = 25
num_buffers = 30

# e.g. "/home/user/git/nebulastream/"
nes_dir = ""
# e.g. "/var/log/nes"
log_dir = ""
# e.g. "/home/user/experiments/influx/"
influx_storage = ""
nes_log_level = LogLevel.INFO
timeout = 60

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
def setup_topology(_nes_dir, _log_dir, _influx_storage, _nes_log_level, _number_workers_producing, _number_workers_not_producing,
                   _number_coordinators, _num_tuples, _num_buffers, _monitoring_type, _timeout):
    info('*** Starting topology\n')
    topo = Topology(_nes_dir, _log_dir, _influx_storage, _nes_log_level, _number_workers_producing, _number_workers_not_producing,
                    _number_coordinators, _num_tuples, _num_buffers, _monitoring_type)
    topo.create_topology()
    topo.start_emulation()
    topo.wait_until_topology_is_complete(_timeout)
    return topo


def execute_experiment(_topology, _influx_db, _influx_table, _iterations, _iterations_before_execution,
                       _monitoring_frequency, _description, _version, _run_cli, _sleep_time, _store_measurements):
    info('*** Executing experiment with following parameters producers=' + str(worker_producing)
         + '; non_producers=' + str(worker_not_producing) + '; monitoring_type=' + monitoring.value + '\n')

    experiment = Experiment(_topology, _influx_db, _influx_table, _iterations, _iterations_before_execution,
                            _monitoring_frequency, _description, _version)
    experiment.start(_store_measurements)


# experiment the executable part
# ---------------------------------------------------------------------
i = 0
run_cli = False
sleep_time = 10
for worker_producing in number_workers_producing:
    for worker_not_producing in number_workers_not_producing:
        for monitoring in monitoring_types:
            info('*** Executing experiment with following parameters producers=' + str(worker_producing)
                 + '; non_producers=' + str(worker_not_producing) + '; monitoring_type=' + monitoring.value + '\n')

            topology = setup_topology(nes_dir, log_dir, influx_storage, nes_log_level, worker_producing,
                                      worker_not_producing, no_coordinators, num_tuples, num_buffers,
                                      monitoring, timeout)

            if run_experiment:
                execute_experiment(topology, influx_db, influx_table, iterations, iterations_before_execution,
                                   monitoring_frequency, description, version, run_cli, sleep_time, store_measurements)

            if i == len(number_workers_producing) * len(number_workers_not_producing) * len(monitoring_types) - 1:
                info('*** Experiment reached last iteration=' + str(i) + ". Activating CLI")
                topology.start_cli()
            else:
                sleep(sleep_time)

            info('*** Stopping network')
            topology.stop_emulation()
            sleep(sleep_time)
            i = i + 1
