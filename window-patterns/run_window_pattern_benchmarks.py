import argparse
import os
import itertools
import subprocess
import sys
import shutil
import re
from datetime import datetime, timedelta
import csv
from time import sleep

# create parser
parser = argparse.ArgumentParser(
    description="Python script to run the window pattern optimization benchmarks."
)
# argument for the path to the project directory
parser.add_argument(
    "--project_dir",
    metavar="project_dir",
    type=str,
    help="Path to the project directory",
)
# parse arguments
args = parser.parse_args()


# path to nebuli
nebuli_path = os.path.join(args.project_dir, "build", "nes-nebuli", "nes-nebuli")
# path to single node worker
single_node_path = os.path.join(
    args.project_dir, "build", "nes-single-node-worker", "nes-single-node-worker"
)
# path to tcp server
# tcp_server_path = os.path.join(
#     args.project_dir, "window-patterns", "tcp-server-tuples.py"
# )
tcp_server_path = os.path.join(
    args.project_dir, "window-patterns", "tcp_server.cpp"
)
# path to yaml config
yaml_config_path = os.path.join(
    args.project_dir, "window-patterns", "config", "joinTumblingConfig.yaml"
)
# output directory path
output_dir = "results/"
# path to the log file
log_path = os.path.join(output_dir, "log.txt")
# number of runs each
number_of_runs = 1
measure_interval_in_seconds = 13

PRELOG = "numactl -N 0 -m 0"


############## config params ##############
QUERIES = []

BUFFER_SIZE_PARAMS = [100000, 48000, 16000]

WORKER_THREADS_PARAMS = [1, 2, 4, 8]

TCP_SERVER_PARAMS = [
    {"time_step": 1, "unorderedness": 0.25, "min_delay": 100, "max_delay": 500},
    {"time_step": 1, "unorderedness": 0.5, "min_delay": 100, "max_delay": 500},
    {"time_step": 1, "unorderedness": 0.75, "min_delay": 100, "max_delay": 500},
    {"time_step": 1, "unorderedness": 1, "min_delay": 100, "max_delay": 500},
    # {"time_step": 100, "unorderedness": 0, "min_delay": 0, "max_delay": 0},
    {"time_step": 1, "unorderedness": 0, "min_delay": 0, "max_delay": 0},
]

SLICE_CACHE_PARAMS = [
    {"slice_cache_type": "DEFAULT"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 0.25},
    {"slice_cache_type": "FIFO", "slice_cache_size": 0.5},
    {"slice_cache_type": "FIFO", "slice_cache_size": 0.75},
    {"slice_cache_type": "LRU", "slice_cache_size": 0.25},
    {"slice_cache_type": "LRU", "slice_cache_size": 0.5},
    {"slice_cache_type": "LRU", "slice_cache_size": 0.75},
]

SORTING_PARAMS = []

static_params = {
    "schema_size": 32,
    "task_queue_size": 1000,
    "buffers_in_global_buffer_manager": 102400,  # 1024 / 5000000
    "buffers_per_worker": 12800,  # 128 / 5000000
    "buffers_in_source_local_buffer_pool": 6400,  # 64 / 312500
    "log_level": "LOG_NONE", # LOG_DEBUG / LOG_NONE
    "nautilus_backend": "COMPILER",
    "number_of_sources": 1,
    "interval": 0.00001,  # 0.001
    "count_limit": 30000000,  # 100000
}


############## variables for statistics ##############
original_statistics_path = "statistics.txt"
statistics_csv = os.path.join(output_dir, "statistics.csv")

csv_fieldnames = [
    "experimentID",
    "benchmarkName",
    "schemaSize",
    "processedTuples",
    "processedTasks",
    "processedBuffers",
    "latencyInMs",
    "tuplesPerSecond",
    "tasksPerSecond",
    "buffersPerSecond",
    "mebiBPerSecond",
    "numberOfWorkerOfThreads",
    "numberOfDeployedQueries",
    "numberOfSources",
    "bufferSizeInBytes",
    "sourceName",
    "queryString",
]

start_pattern = re.compile(
    r"(?P<timestamp>[\d-]+ [\d:.]+) Task (?P<task_id>\d+) for Pipeline (?P<pipeline_id>\d+) of Query (?P<query_id>\d+) Started with no. of tuples: (?P<tuples>\d+)"
)
end_pattern = re.compile(
    r"(?P<timestamp>[\d-]+ [\d:.]+) Task (?P<task_id>\d+) for Pipeline (?P<pipeline_id>\d+) of Query (?P<query_id>\d+) Completed."
)

pipelines_file = "pipelines.txt"


def build():
    # build and compile nes-nebuli
    build_dir = os.path.join(args.project_dir, "build")
    subprocess.run(
        [
            "cmake",
            "--build",
            build_dir,
            "--config",
            "Release",
            "--target",
            "nes-nebuli",
        ],
        check=True,
    )
    print("Build of nes-nebuli successful!")

    # copy executable to current directory and update variable
    global nebuli_path
    new_nebuli_path = os.path.join(os.getcwd(), "nebuli")
    os.system(f"cp {nebuli_path} {new_nebuli_path}")
    nebuli_path = new_nebuli_path

    # build and compile nes-single-node-worker
    subprocess.run(
        [
            "cmake",
            "--build",
            build_dir,
            "--config",
            "Release",
            "--target",
            "nes-single-node-worker",
        ],
        check=True,
    )
    print("Build of nes-single-node-worker successful!")

    # copy executable to current directory and update variable
    global single_node_path
    new_single_node_path = os.path.join(os.getcwd(), "single-node")
    os.system(f"cp {single_node_path} {new_single_node_path}")
    single_node_path = new_single_node_path

    # build and compile tcp_server
    global tcp_server_path
    subprocess.run(
        [
            "clang++", tcp_server_path, "-o", "tcp_server"
        ],
        check=True,
    )
    print("Build of tcp_server successful!")
    print("\n")

    # update tcp server path
    tcp_server_path = os.path.join(os.getcwd(), "tcp_server")


def run_benchmark(number_of_runs=1):
    experiment_id = 0
    # TODO: use number_of_runs
    single_node_params = list(
        itertools.product(BUFFER_SIZE_PARAMS, WORKER_THREADS_PARAMS)
    )
    for single_node_config in single_node_params:
        current_params = dict(static_params)
        current_params["query"] = "Join"
        args = {
            "buffer_size": single_node_config[0],
            "worker_threads": single_node_config[1],
        }
        current_params.update(args)
        # start single node for each worker thread and buffer size combination
        for slice_cache_config in SLICE_CACHE_PARAMS:
            current_params.update(**slice_cache_config)
            # generate slice cache name for benchmark name
            slice_cache_name = slice_cache_config["slice_cache_type"]
            if "slice_cache_size" in slice_cache_config:
                slice_cache_name += str(slice_cache_config["slice_cache_size"])
                # adapt slice cache size to buffer size
                current_params["slice_cache_size"] = int(
                    slice_cache_config["slice_cache_size"]
                    * current_params["buffer_size"]
                )
            for server_config in TCP_SERVER_PARAMS:
                current_params.update(**server_config)
                # generate source name and benchmark name
                source_name = ""
                if server_config["unorderedness"] != 0:
                    source_name = "OutOfOrder" + str(
                        int(server_config["unorderedness"] * 100)
                    )
                else:
                    source_name = "TimeStep" + str(server_config["time_step"])
                current_params["experiment_id"] = experiment_id
                current_params["source_name"] = source_name
                current_params["benchmark_name"] = (
                    current_params["query"] + slice_cache_name + source_name
                )
                log_params(current_params)

                print(f"Starting experiment with ID {experiment_id}!")
                print("\n")

                # start single node
                single_node_process = start_single_node(current_params)

                # start tcp servers
                tcp_server_processes = start_tcp_server(5010, current_params)
                processes = [single_node_process] + tcp_server_processes
                if current_params["query"] == "Join":
                    tcp_server_2_processes = start_tcp_server(5011, current_params)
                    processes.extend(tcp_server_2_processes)

                # run queries
                launch_query(experiment_id)
                confirm_execution_and_sleep(single_node_process)
                stop_query()
                # terminate processes
                terminate_processes(processes)
                # create csv from statistics
                read_statistics(original_statistics_path, current_params)
                calculate_and_write_to_csv(current_params)

                print(f"Experiment with ID {experiment_id} finished!")
                print("\n")
                experiment_id += 1


def log_params(current_params):
    with open(log_path, "a") as file:
        file.write(f"Experiment ID: {current_params['experiment_id']}\n")
        for key, value in current_params.items():
            file.write(f"{key}: {value}, ")
        file.write("\n")


def start_single_node(current_params):
    args = [
        f"--worker.queryEngine.numberOfWorkerThreads={current_params['worker_threads']}",
        f"--worker.queryEngine.taskQueueSize={current_params['task_queue_size']}",
        f"--worker.bufferSizeInBytes={current_params['buffer_size']}",
        f"--worker.numberOfBuffersInGlobalBufferManager={current_params['buffers_in_global_buffer_manager']}",
        f"--worker.numberOfBuffersPerWorker={current_params['buffers_per_worker']}",
        f"--worker.numberOfBuffersInSourceLocalBufferPool={current_params['buffers_in_source_local_buffer_pool']}",
        f"--worker.logLevel={current_params['log_level']}",
        f"--worker.queryCompiler.nautilusBackend={current_params['nautilus_backend']}",
        f"--worker.queryCompiler.sliceCacheType={current_params['slice_cache_type']}",
    ]

    if "slice_cache_size" in current_params:
        args.append(
            f"--worker.queryCompiler.sliceCacheSize={current_params['slice_cache_size']}"
        )

    cmd = PRELOG.split() + [single_node_path] + args
    print(f"Executing command: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    # confirm starting single node worker was successful
    success_message = "Single node worker successfully started."
    confirm_execution_and_sleep(process, success_message, 3)

    return process


def start_tcp_server(port, current_params):
    processes = []
    env = os.environ.copy()

    for i in range(current_params["number_of_sources"]):
        current_port = port + i
        args = [
            f"-p {current_port}",
            f"-i {current_params['interval']}",
            f"-n {current_params['count_limit']}",
            f"-t {current_params['time_step']}",
        ]

        if "unorderedness" in current_params:
            args.append(f"-u {current_params['unorderedness']}")
            args.append(f"-d {current_params['min_delay']}")
            args.append(f"-m {current_params['max_delay']}")

        # cmd = [sys.executable, tcp_server_path] + args
        cmd = [tcp_server_path] + args
        print(f"Executing command: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        processes.append(process)
        # confirm starting tcp server was successful
        success_message = f"TCP server successfully started on port {current_port}."
        confirm_execution_and_sleep(process, success_message, 3)

    return processes


def confirm_execution_and_sleep(process, success_message=None, sleep_time=0):
    retcode = process.poll()
    if retcode is None:
        if success_message is not None:
            print(success_message + f" Process ID: {process.pid}\n")
        sleep(sleep_time)
    else:
        print(f"Process failed with return code {retcode}:")
        stdout, stderr = process.communicate()
        print(f"Error in process: {stderr}")


def launch_query(experiment_id):
    print(f"Launching query")
    cmd = [
        # "sh",
        # "-c",
        f"cat {yaml_config_path} | sed 's#GENERATE#{output_dir}sink/query_{experiment_id}.csv#' | {nebuli_path} register -x -s localhost:8080",
    ]
    print(f"Executing command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sleep(measure_interval_in_seconds)
    except Exception as e:
        print(f"Error {e}: {e.stderr.decode()}")


def stop_query():
    print(f"Stopping query")
    cmd = [f"{nebuli_path} stop 1 -s localhost:8080"] # ["sh", "-c", f"{nebuli_path} stop 1 -s localhost:8080"]
    print(f"Executing command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        print(f"Error {e}: {e.stderr.decode()}")
    print("\n")


def terminate_processes(processes):
    for process in processes:
        retcode = process.poll()
        if retcode is None:
            # terminate manually
            process.terminate()
            print(f"Terminated process {process.pid}")
        else:
            print(f"Process {process.pid} failed with return code {retcode}:")
            stdout, stderr = process.communicate()
            print(f"Error in process {process.pid} {stderr}")
    print("Successfully terminated all processes!\n")


def read_statistics(statistics_file, current_params):
    print("Now reading the statistics... ")
    # extract relevant pipeline ids
    current_params["pipeline_ids"] = []
    with open(pipelines_file, "r") as file:
        for line in file:
            if "StreamJoinBuild" in line:
                parts = line.split("pipelineId: ")
            if len(parts) > 1:
                pipeline_id = parts[1].strip()
                current_params["pipeline_ids"].append(pipeline_id)
    # move file to output directory
    shutil.move(
        pipelines_file,
        os.path.join(
            output_dir,
            "statistics",
            os.path.basename(statistics_file).replace(
                ".txt", f"_{current_params['experiment_id']}.txt"
            ),
        ),
    )

    with open(statistics_file, "r") as file:
        processed_tuples = 0
        completed_tasks = set()
        start_times = {}
        time_sum = 0
        start_ts = datetime.strptime(
            start_pattern.match(file.readline()).group("timestamp"),
            "%Y-%m-%d %H:%M:%S.%f",
        ) + timedelta(seconds=2)
        # parse each line of the statistics file
        for line in file:
            start_match = start_pattern.match(line)
            end_match = end_pattern.match(line)

            if start_match and start_match.group("pipeline_id") in current_params["pipeline_ids"]:
                start_timestamp = datetime.strptime(
                    start_match.group("timestamp"),
                    "%Y-%m-%d %H:%M:%S.%f",
                )
                # if start_timestamp < start_ts:
                #     continue
                processed_tuples += int(start_match.group("tuples"))
                id = (
                    start_match.group("task_id")
                    + "/"
                    + start_match.group("pipeline_id")
                    + "/"
                    + start_match.group("query_id")
                )
                start_times[id] = start_timestamp
            elif end_match and end_match.group("pipeline_id") in current_params["pipeline_ids"]:
                end_timestamp = datetime.strptime(
                    end_match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f"
                )
                # if end_timestamp < start_ts:
                #     continue
                id = (
                    end_match.group("task_id")
                    + "/"
                    + end_match.group("pipeline_id")
                    + "/"
                    + end_match.group("query_id")
                )
                if id in start_times:
                    time_sum += (end_timestamp - start_times[id]).total_seconds()
                completed_tasks.add(end_match.group("task_id"))

        current_params["processed_tuples"] = processed_tuples
        current_params["processed_tasks"] = len(completed_tasks)
        current_params["latency"] = time_sum
    # move file to output directory
    shutil.move(
        statistics_file,
        os.path.join(
            output_dir,
            "statistics",
            os.path.basename(statistics_file).replace(
                ".txt", f"_{current_params['experiment_id']}.txt"
            ),
        ),
    )


def calculate_and_write_to_csv(current_params):
    print(f"Writing statistics to {statistics_csv}\n")
    with open(statistics_csv, "a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)

        # calculate latency and throughput
        tuples = current_params["processed_tuples"]
        latency = current_params["latency"] * 1000  # latency in ms
        throughput = tuples / (latency / 1000)  # throughput in tuples/s

        # calculate tasks and buffers per second
        tasks = current_params["processed_tasks"]
        tasks_per_second = tasks / (latency / 1000)

        buffer_size_in_tuples = (
            current_params["buffer_size"] / current_params["schema_size"]
        )
        buffers = tuples / buffer_size_in_tuples
        buffers_per_second = buffers / (latency / 1000)

        mebi_per_second = (throughput * 32) / (1024 * 1024)

        writer.writerow(
            {
                "experimentID": current_params["experiment_id"],
                "benchmarkName": current_params["benchmark_name"],
                "schemaSize": current_params["schema_size"],
                "processedTuples": tuples,
                "processedTasks": tasks,
                "processedBuffers": buffers,
                "latencyInMs": latency,
                "tuplesPerSecond": throughput,
                "tasksPerSecond": tasks_per_second,
                "buffersPerSecond": buffers_per_second,
                "mebiBPerSecond": mebi_per_second,
                "numberOfWorkerOfThreads": current_params["worker_threads"],
                "numberOfDeployedQueries": 1,
                "numberOfSources": current_params["number_of_sources"],
                "bufferSizeInBytes": current_params["buffer_size"],
                "sourceName": current_params["source_name"],
                "queryString": current_params["query"],
            }
        )


def main():
    # build all executables
    build()

    # create output directory
    if not os.path.exists(output_dir):
        os.makedirs(os.path.join(output_dir, "sink"))
        os.makedirs(os.path.join(output_dir, "statistics"))

    # create statistics csv
    with open(statistics_csv, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        writer.writeheader()

    run_benchmark(number_of_runs)

    print(f"Benchmarks done. See results in {output_dir}.")


if __name__ == "__main__":
    main()
