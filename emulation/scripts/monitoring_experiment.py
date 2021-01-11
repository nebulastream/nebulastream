from lib.topology import Topology, LogLevel
from lib.experiment import MonitoringExperiment
from lib.util import MonitoringType

from mininet.log import info, setLogLevel
from time import sleep

setLogLevel('info')

# repeatable parameters for each experiment
# ---------------------------------------------------------------------
number_workers_producing = [5]
number_workers_not_producing = [0, 4, 9, 19, 44]
# MonitoringType.DISABLED, MonitoringType.NEMO_PULL, MonitoringType.PROMETHEUS
monitoring_types = [MonitoringType.DISABLED, MonitoringType.NEMO_PULL, MonitoringType.PROMETHEUS]
store_measurements = True
run_experiment = True

# topology parameters
# ---------------------------------------------------------------------
num_tuples = 25
num_buffers = 30

# e.g. /home/user/git/nebulastream/
nes_dir = ""

# e.g. /var/log/nes
log_dir = ""

# e.g. /home/user/experiments/influx/
influx_storage = ""

nes_log_level = LogLevel.INFO
topology_creation_timeout = 120

# experiment parameters
# ---------------------------------------------------------------------
version = "1"
iterations = 60
iterations_before_execution = 0
monitoring_frequency = 1
coordinators = 1
influx_db = "monitoring"
influx_table = "monitoring_measurement_ysb"
description = str(num_tuples) + "Tup-" + str(num_buffers) + "Buf"
experiment_class = MonitoringExperiment


# experiment method definitions
# ---------------------------------------------------------------------
def setup_topology(t_nes_dir, t_log_dir, t_influx_storage, t_nes_log_level, t_number_workers_producing,
                   t_number_workers_not_producing,
                   t_number_coordinators, t_num_tuples, t_num_buffers, t_monitoring_type, _timeout):
    info('*** Starting topology\n')
    topo = Topology(t_nes_dir, t_log_dir, t_influx_storage, t_nes_log_level, t_number_workers_producing,
                    t_number_workers_not_producing,
                    t_number_coordinators, t_num_tuples, t_num_buffers, t_monitoring_type)
    topo.create_topology()
    topo.start_emulation()
    complete = topo.wait_until_topology_is_complete(_timeout)

    if not complete:
        print("Error creating topology")

    return complete, topo


def execute_experiment(_experiment_class, _topology, _influx_db, _influx_table, _iterations,
                       _iterations_before_execution,
                       _monitoring_frequency, _description, _version, _run_cli, _sleep_time_after_deployment,
                       _store_measurements):
    info('*** Executing experiment with following parameters producers=' + str(_topology.number_workers_producing)
         + '; non_producers=' + str(
        _topology.number_workers_not_producing) + '; monitoring_type=' + _topology.monitoring_type.value + '\n')

    experiment = _experiment_class(_topology, _influx_db, _influx_table, _iterations, _iterations_before_execution,
                                   _monitoring_frequency, _description, _version)
    experiment.start(_store_measurements, _sleep_time_after_deployment)


# experiment the executable part
# ---------------------------------------------------------------------
i = 0
run_cli = False
sleep_time_between_experiments = 10
sleep_time_after_deployment = 1

for worker_producing in number_workers_producing:
    for worker_not_producing in number_workers_not_producing:
        for monitoring_type in monitoring_types:
            info('*** Executing experiment with following parameters producers=' + str(worker_producing)
                 + '; non_producers=' + str(worker_not_producing) + '; monitoring_type=' + monitoring_type.value + '\n')

            success, topology = setup_topology(nes_dir, log_dir, influx_storage, nes_log_level, worker_producing,
                                               worker_not_producing,
                                               coordinators, num_tuples, num_buffers, monitoring_type,
                                               topology_creation_timeout)

            if success and run_experiment:
                try:
                    execute_experiment(experiment_class, topology, influx_db, influx_table, iterations,
                                       iterations_before_execution,
                                       monitoring_frequency, description, version, run_cli, sleep_time_after_deployment,
                                       store_measurements)
                except Exception as e:
                    print("Exception occurred during experiment: ", e)
                    topology.stop_emulation()
                    raise RuntimeError("Experiments aborted due to erroneous experiment")

            if i == len(number_workers_producing) * len(number_workers_not_producing) * len(monitoring_types) - 1:
                info('*** Experiment reached last iteration=' + str(i) + ". Activating CLI")
                topology.start_cli()
            else:
                sleep(sleep_time_between_experiments)

            info('*** Stopping network')
            topology.stop_emulation()
            sleep(sleep_time_between_experiments)
            i = i + 1
