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
parser.add_argument(
    "--no_execution",
    action="store_true",
    help="Wether the commands should be executed or not",
)
parser.add_argument(
    "--debug",
    action="store_true",
    help="Activate debug mode",
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
tcp_server_path = os.path.join(
    args.project_dir, "window-patterns", "tcp-server-tuples.py"
)
# tcp_server_path = os.path.join(args.project_dir, "window-patterns", "tcp_server.cpp")
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
measure_interval_in_seconds = 30
# build and compile configurations
releaseOrDebug = "Release"
NES_LOG_LEVEL = "LEVEL_NONE"
log_level = "LOG_NONE"


PRELOG = "numactl -N 0 -m 0"


############## config params ##############
QUERIES = []

BUFFER_SIZE_PARAMS = [100000, 48000, 16000]

WORKER_THREADS_PARAMS = [8, 4, 2, 1]

UNORDEREDNESS_PARAMS = [
    {"unorderedness": 0.25, "min_delay": 100, "max_delay": 500},
    {"unorderedness": 0.5, "min_delay": 100, "max_delay": 500},
    {"unorderedness": 0.75, "min_delay": 100, "max_delay": 500},
    {"unorderedness": 1, "min_delay": 100, "max_delay": 500},
    {"unorderedness": 0, "min_delay": 0, "max_delay": 0},
]

SLICE_STORE_PARAMS = ["MAP", "LIST"]

SLICE_CACHE_PARAMS = [
    {"slice_cache_type": "DEFAULT"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 4, "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 8, "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 15, "lock_slice_cache": "false"},
    {"slice_cache_type": "LRU", "slice_cache_size": 4, "lock_slice_cache": "false"},
    {"slice_cache_type": "LRU", "slice_cache_size": 8, "lock_slice_cache": "false"},
    {"slice_cache_type": "LRU", "slice_cache_size": 15, "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 8, "lock_slice_cache": "true"},
    {"slice_cache_type": "LRU", "slice_cache_size": 8, "lock_slice_cache": "true"},
]

SORTING_PARAMS = [
    {"sort_buffer_by_field": ""},
    # {"sort_buffer_by_field": "timestamp"},
    # {"sort_buffer_by_field": "value"},
]

static_params = {
    "schema_size": 32,
    "task_queue_size": 10000,  # 1000
    "buffers_in_global_buffer_manager": 102400,  # 1024 / 5000000
    "buffers_per_worker": 12800,  # 128 / 5000000
    "buffers_in_source_local_buffer_pool": 6400,  # 64 / 312500
    "log_level": log_level,  # LOG_DEBUG / LOG_NONE
    "nautilus_backend": "COMPILER",
    "number_of_sources": 1,
    "interval": 0.00001,  # 0.001
    "count_limit": 30000000,  # 100000
    "total_processed_tuples": 0,
    "total_latency": 0,
    "total_processed_tasks": 0,
    "tuples_for_200_tasks": 0,
    "latency_for_200_tasks": 0,
    "tuples_for_ten_sec": 0,
    "exact_latency_for_ten_sec": 0,
    "tasks_for_ten_sec": 0,
}


############## variables for statistics ##############
original_statistics_path = "statistics.txt"
statistics_csv = os.path.join(output_dir, "statistics.csv")

csv_fieldnames = [
    "experimentID",
    "benchmarkName",
    "sourceName",
    "bufferSizeInBytes",
    "numberOfWorkerOfThreads",
    "totalProcessedTuples",
    "totalProcessedTasks",
    "totalProcessedBuffers",
    "totalLatencyInMs",
    "tuplesPerSecond",
    "tasksPerSecond",
    "buffersPerSecond",
    "mebiBPerSecond",
    "avgThroughput200Tasks",
    "latency200Tasks",
    "avgThroughput10Sec",
    "tasksPer10Sec",
    "measureIntervalInSeconds",
    "numberOfDeployedQueries",
    "numberOfSources",
    "schemaSize",
    "query",
]

task_statistics_fieldnames = [
    "taskId",
    "pipelineId",
    "startTime",
    "endTime",
    "processedTuples",
    "latencyInMs",
    "tuplesPerSecond",
]

start_pattern = re.compile(
    r"(?P<timestamp>[\d-]+ [\d:.]+) Task (?P<task_id>\d+) for Pipeline (?P<pipeline_id>\d+) of Query (?P<query_id>\d+) Started with no. of tuples: (?P<tuples>\d+)"
)
end_pattern = re.compile(
    r"(?P<timestamp>[\d-]+ [\d:.]+) Task (?P<task_id>\d+) for Pipeline (?P<pipeline_id>\d+) of Query (?P<query_id>\d+) Completed."
)

pipelines_file = "pipelines.txt"


def build():
    cmd = [
        "cmake",
        f"-DCMAKE_BUILD_TYPE:STRING={releaseOrDebug}",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE",
        "-DCMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++",
        "-DNES_USE_LLD=OFF",
        "-DUSE_CCACHE_IF_AVAILABLE=OFF",
        f"-DNES_LOG_LEVEL={NES_LOG_LEVEL}",  # "-DNES_LOG_LEVEL=LEVEL_NONE/DEBUG",
        "-DCMAKE_TOOLCHAIN_FILE=/ssd/vcpkg/scripts/buildsystems/vcpkg.cmake",
        "--no-warn-unused-cli",
        f"-S {args.project_dir}",
        f"-B {os.path.join(args.project_dir, 'build')}",
        # "-DCMAKE_MAKE_PROGRAM=/usr/bin/make",
        "-DCMAKE_MAKE_PROGRAM=/usr/bin/ninja",
        "-G Ninja",
    ]
    print(f"Executing command: {' '.join(cmd)}")
    subprocess.run(cmd)

    # build and compile nes-nebuli
    build_dir = os.path.join(args.project_dir, "build")
    subprocess.run(
        [
            "cmake",
            "--build",
            build_dir,
            "--config",
            releaseOrDebug,
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
            releaseOrDebug,
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
    # global tcp_server_path
    # subprocess.run(
    #     ["clang++", tcp_server_path, "-o", "tcp_server"],
    #     check=True,
    # )
    # print("Build of tcp_server successful!")
    # print("\n")

    # # update tcp server path
    # tcp_server_path = os.path.join(os.getcwd(), "tcp_server")


def run_benchmark(number_of_runs=1):
    experiment_id = 0
    # TODO: use number_of_runs

    # start tcp servers
    count_limit = static_params["count_limit"]
    tcp_server_process = start_tcp_server(5010, count_limit)
    processes = [tcp_server_process]
    tcp_server_2_process = start_tcp_server(5011, count_limit)
    processes.append(tcp_server_2_process)

    single_node_params = list(
        itertools.product(BUFFER_SIZE_PARAMS, WORKER_THREADS_PARAMS)
    )
    for single_node_config in single_node_params:
        current_params = dict(static_params)
        current_params["query"] = "Join"
        arguments = {
            "buffer_size": single_node_config[0],
            "worker_threads": single_node_config[1],
        }
        current_params.update(arguments)
        for slice_store_type in SLICE_STORE_PARAMS:
            current_params["slice_store_type"] = slice_store_type
            # start single node for each worker thread and buffer size combination
            for slice_cache_config in SLICE_CACHE_PARAMS:
                current_params.update(**slice_cache_config)
                # generate slice cache name for benchmark name
                slice_cache_name = slice_cache_config["slice_cache_type"]
                if "slice_cache_size" in slice_cache_config:
                    slice_cache_name += str(slice_cache_config["slice_cache_size"])
                    if slice_cache_config["lock_slice_cache"] == "true":
                        slice_cache_name += "Locked"
                current_params["sort_buffer_by_field"] = ""  # TODO: sort buffer
                for unorderedness_config in UNORDEREDNESS_PARAMS:
                    if (
                        current_params["worker_threads"] == 1
                        and unorderedness_config["unorderedness"] != 0
                    ):
                        continue
                    current_params.update(**unorderedness_config)
                    # generate source name and benchmark name
                    source_name = "TimeStep1"
                    if unorderedness_config["unorderedness"] != 0:
                        source_name = "OutOfOrder" + str(
                            int(unorderedness_config["unorderedness"] * 100)
                        )
                    current_params["experiment_id"] = experiment_id
                    current_params["source_name"] = source_name
                    current_params["benchmark_name"] = (
                        slice_store_type + slice_cache_name
                    )

                    print(f"Starting experiment with ID {experiment_id}!")
                    print("\n")

                    # start single node
                    single_node_process = start_single_node(current_params)

                    # run queries
                    query_id = launch_query(experiment_id)
                    # confirm_execution_and_sleep(single_node_process)
                    if query_id is not None:
                        stop_query(query_id)

                    # wait for input before processing data
                    if args.no_execution:
                        print("Now waiting for input.")
                        input()

                    # terminate processes
                    terminate_processes([single_node_process])

                    # create csv from statistics
                    read_statistics(original_statistics_path, current_params)
                    calculate_and_write_to_csv(current_params)

                    log_params(current_params)
                    reset_statistic_params(current_params)

                    print(f"Experiment with ID {experiment_id} finished!\n")
                    experiment_id += 1

    terminate_processes(processes)


def start_single_node(current_params):
    arguments = [
        f"--worker.queryEngine.numberOfWorkerThreads={current_params['worker_threads']}",
        f"--worker.queryEngine.taskQueueSize={current_params['task_queue_size']}",
        f"--worker.bufferSizeInBytes={current_params['buffer_size']}",
        f"--worker.numberOfBuffersInGlobalBufferManager={current_params['buffers_in_global_buffer_manager']}",
        f"--worker.numberOfBuffersPerWorker={current_params['buffers_per_worker']}",
        f"--worker.numberOfBuffersInSourceLocalBufferPool={current_params['buffers_in_source_local_buffer_pool']}",
        f"--worker.logLevel={current_params['log_level']}",
        f"--worker.queryCompiler.nautilusBackend={current_params['nautilus_backend']}",
        f"--worker.queryCompiler.unorderedness={current_params['unorderedness']}",
        f"--worker.queryCompiler.minDelay={current_params['min_delay']}",
        f"--worker.queryCompiler.maxDelay={current_params['max_delay']}",
        f"--worker.queryCompiler.sliceStoreType={current_params['slice_store_type']}",
        f"--worker.queryCompiler.sliceCacheType={current_params['slice_cache_type']}",
    ]

    if "slice_cache_size" in current_params:
        arguments.append(
            f"--worker.queryCompiler.sliceCacheSize={current_params['slice_cache_size']}"
        )
        arguments.append(
            f"--worker.queryCompiler.lockSliceCache={current_params['lock_slice_cache']}",
        )

    if current_params["sort_buffer_by_field"] != "":
        arguments.append(
            f"--worker.queryCompiler.sortBufferByField={current_params['sort_buffer_by_field']}"
        )

    cmd = PRELOG.split() + [single_node_path] + arguments
    print(f"Executing command: {' '.join(cmd)}")

    if args.no_execution:
        return []

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    # confirm starting single node worker was successful
    success_message = "Single node worker successfully started."
    confirm_execution_and_sleep(process, success_message, 3)

    return process


def start_tcp_server(port, count_limit):
    env = os.environ.copy()

    arguments = [
        f"-p {port}",
        f"-n {count_limit}",
    ]

    cmd = [sys.executable, tcp_server_path] + arguments
    # cmd = [tcp_server_path] + arguments
    print(f"Executing command: {' '.join(cmd)}")

    if args.no_execution:
        return []

    process = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    # confirm starting tcp server was successful
    success_message = f"TCP server successfully started on port {port}."
    confirm_execution_and_sleep(process, success_message, 3)

    return process


def confirm_execution_and_sleep(process, success_message=None, sleep_time=0):
    if not args.no_execution:
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

    if args.no_execution:
        return None

    try:
        query_id = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )  # , executable="/bin/bash")
        sleep(measure_interval_in_seconds)
        return query_id.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        print(f"Standard Error: {e.stderr}")
    except Exception as e:
        print(f"Error {e}: {e.stderr.decode()}")


def stop_query(query_id):
    print(f"Stopping query")
    cmd = [
        f"{nebuli_path} stop -s localhost:8080 {query_id}"
        # "sh", "-c", f"{nebuli_path} stop 1 -s localhost:8080"
    ]
    print(f"Executing command: {' '.join(cmd)}")

    if args.no_execution:
        return

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )  # , executable="/bin/bash")
        print(f"Execution done {result.stdout}")
    except subprocess.TimeoutExpired:
        print("Process timed out.")
        with open(log_path, "a") as file:
            file.write("used timeout\n")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        print(f"Standard Error: {e.stderr}")
    except Exception as e:
        print(f"Error {e}: {e.stderr.decode()}")
    print("\n")


def terminate_processes(processes):
    if not args.no_execution:
        for process in processes:
            print(f"Terminating process {process.pid}")
            retcode = process.poll()
            if retcode is None:
                # terminate manually
                process.terminate()
                process.wait()
                print(f"Terminated process {process.pid}")
            else:
                print(f"Process {process.pid} failed with return code {retcode}:")
                process.terminate()
                stdout, stderr = process.communicate()
                print(f"Error in process {process.pid} {stderr}")
        print("Successfully terminated all processes!\n")


def read_statistics(statistics_file, current_params):
    print("Now reading the statistics... ")

    experiment_id = current_params["experiment_id"]

    # extract relevant pipeline ids
    current_params["pipeline_ids"] = []
    if os.path.exists(pipelines_file):
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
                os.path.basename(pipelines_file).replace(
                    ".txt", f"_{experiment_id}.txt"
                ),
            ),
        )

    if os.path.exists(statistics_file):
        with open(statistics_file, "r") as file:
            processed_tuples = {}
            time_sum = 0
            completed_tasks = set()
            start_times = {}
            end_times = {}
            # For individual statistics
            latencies = {}
            pipelines = {}
            first_match = True
            start_ts = datetime.now()
            end_ts = start_ts + timedelta(seconds=10)
            task_count_reached = False
            ten_sec_reached = False
            processed_tuples_with_warmup = 0
            time_sum_with_warmup = 0
            completed_tasks_with_warmup = 0

            # parse each line of the statistics file
            for line in file:
                start_match = start_pattern.match(line)
                end_match = end_pattern.match(line)

                if (
                    start_match
                    and start_match.group("pipeline_id")
                    in current_params["pipeline_ids"]
                ):
                    start_timestamp = datetime.strptime(
                        start_match.group("timestamp"),
                        "%Y-%m-%d %H:%M:%S.%f",
                    )
                    if first_match:
                        start_ts = start_timestamp + timedelta(seconds=1)
                        end_ts = start_ts + timedelta(seconds=10)
                        first_match = False
                    id = int(start_match.group("task_id"))
                    start_times[id] = start_timestamp
                    pipelines[id] = start_match.group("pipeline_id")
                    number_of_tuples = int(start_match.group("tuples"))
                    processed_tuples[id] = number_of_tuples
                    if first_match is False and start_timestamp >= start_ts:
                        processed_tuples_with_warmup += number_of_tuples
                elif (
                    end_match
                    and end_match.group("pipeline_id") in current_params["pipeline_ids"]
                ):
                    end_timestamp = datetime.strptime(
                        end_match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f"
                    )
                    # if end_timestamp < start_ts:
                    #     continue
                    id = int(end_match.group("task_id"))
                    if id in start_times.keys():
                        completed_tasks.add(id)
                        end_times[id] = end_timestamp
                        difference = (end_timestamp - start_times[id]).total_seconds()
                        latencies[id] = difference
                        time_sum += difference

                        if end_timestamp >= start_ts:
                            time_sum_with_warmup += difference
                            completed_tasks_with_warmup += 1

                        if (
                            completed_tasks_with_warmup >= 200
                            and task_count_reached is False
                        ):
                            current_params["tuples_for_200_tasks"] = (
                                processed_tuples_with_warmup
                            )
                            current_params["latency_for_200_tasks"] = (
                                time_sum_with_warmup
                            )
                            task_count_reached = True

                        if end_timestamp >= end_ts and ten_sec_reached is False:
                            current_params["tuples_for_ten_sec"] = (
                                processed_tuples_with_warmup
                            )
                            current_params["exact_latency_for_ten_sec"] = (
                                time_sum_with_warmup
                            )
                            current_params["tasks_for_ten_sec"] = (
                                completed_tasks_with_warmup
                            )
                            ten_sec_reached = True

            current_params["total_processed_tuples"] = sum(processed_tuples.values())
            current_params["total_latency"] = time_sum
            current_params["total_processed_tasks"] = len(completed_tasks)
            if task_count_reached is False:
                current_params["tuples_for_200_tasks"] = processed_tuples_with_warmup
                current_params["latency_for_200_tasks"] = time_sum_with_warmup
            if ten_sec_reached is False:
                current_params["tuples_for_ten_sec"] = processed_tuples_with_warmup
                current_params["exact_latency_for_ten_sec"] = time_sum_with_warmup
                current_params["tasks_for_ten_sec"] = completed_tasks_with_warmup

            # save individual task statistics
            task_statistics_path = os.path.join(
                output_dir, "statistics", f"statistics_{experiment_id}.csv"
            )
            with open(task_statistics_path, "w", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=task_statistics_fieldnames)
                writer.writeheader()

                sorted_tasks = sorted(completed_tasks)
                for id in sorted_tasks:
                    # calculate latency and throughput
                    tuples = processed_tuples[id]
                    latency = float(latencies[id] * 1000)  # latency in ms
                    throughput = 0
                    if tuples != 0 and latency != 0:
                        throughput = tuples / (latency / 1000)  # throughput in tuples/s
                    writer.writerow(
                        {
                            "taskId": id,
                            "pipelineId": pipelines[id],
                            "startTime": start_times[id],
                            "endTime": end_times[id],
                            "processedTuples": tuples,
                            "latencyInMs": latency,
                            "tuplesPerSecond": throughput,
                        }
                    )

        # move file to output directory
        shutil.move(
            statistics_file,
            os.path.join(
                output_dir,
                "statistics",
                os.path.basename(statistics_file).replace(
                    ".txt", f"_{experiment_id}.txt"
                ),
            ),
        )


def calculate_and_write_to_csv(current_params):
    print(f"Writing statistics to {statistics_csv}\n")
    with open(statistics_csv, "a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)

        # calculate latency and throughput
        tuples = current_params["total_processed_tuples"]
        latency = current_params["total_latency"]  # latency in ms
        throughput = 0
        if tuples != 0 and latency != 0:
            throughput = tuples / latency  # throughput in tuples/s

        # calculate tasks and buffers per second
        tasks = current_params["total_processed_tasks"]
        tasks_per_second = 0
        if tasks != 0 and latency != 0:
            tasks_per_second = tasks / latency

        buffer_size_in_tuples = (
            current_params["buffer_size"] / current_params["schema_size"]
        )
        buffers = tuples / buffer_size_in_tuples
        buffers_per_second = 0
        if buffers != 0 and latency != 0:
            buffers_per_second = buffers / (latency / 1000)

        mebi_per_second = (throughput * 32) / (1024 * 1024)

        tuples_200 = current_params["tuples_for_200_tasks"]
        latency_200 = current_params["latency_for_200_tasks"]
        throughput_200 = 0
        if tuples_200 != 0 and latency_200 != 0:
            throughput_200 = tuples_200 / latency_200

        tuples_ten_sec = current_params["tuples_for_ten_sec"]
        latency_ten_sec = current_params["exact_latency_for_ten_sec"]
        throughput_ten_sec = 0
        if tuples_ten_sec != 0 and latency_ten_sec != 0:
            throughput_ten_sec = tuples_ten_sec / latency_ten_sec

        writer.writerow(
            {
                "experimentID": current_params["experiment_id"],
                "benchmarkName": current_params["benchmark_name"],
                "sourceName": current_params["source_name"],
                "bufferSizeInBytes": current_params["buffer_size"],
                "numberOfWorkerOfThreads": current_params["worker_threads"],
                "totalProcessedTuples": tuples,
                "totalProcessedTasks": tasks,
                "totalProcessedBuffers": buffers,
                "totalLatencyInMs": latency * 1000,
                "tuplesPerSecond": throughput,
                "tasksPerSecond": tasks_per_second,
                "buffersPerSecond": buffers_per_second,
                "mebiBPerSecond": mebi_per_second,
                "avgThroughput200Tasks": throughput_200,
                "latency200Tasks": latency_200 * 1000,
                "avgThroughput10Sec": throughput_ten_sec,
                "tasksPer10Sec": current_params["tasks_for_ten_sec"],
                "measureIntervalInSeconds": measure_interval_in_seconds,
                "numberOfDeployedQueries": 1,
                "numberOfSources": current_params["number_of_sources"],
                "schemaSize": current_params["schema_size"],
                "query": current_params["query"],
            }
        )


def log_params(current_params):
    with open(log_path, "a") as file:
        file.write(f"Experiment ID: {current_params['experiment_id']}\n")
        for key, value in current_params.items():
            file.write(f"{key}: {value}, ")
        file.write("\n")


def reset_statistic_params(current_params):
    current_params["total_processed_tuples"] = 0
    current_params["total_latency"] = 0
    current_params["total_processed_tasks"] = 0
    current_params["tuples_for_200_tasks"] = 0
    current_params["latency_for_200_tasks"] = 0
    current_params["tuples_for_ten_sec"] = 0
    current_params["exact_latency_for_ten_sec"] = 0
    current_params["tasks_for_ten_sec"] = 0


def main():
    if args.debug:
        global releaseOrDebug, NES_LOG_LEVEL, log_level
        releaseOrDebug = "Debug"
        NES_LOG_LEVEL = "DEBUG"
        log_level = "LOG_DEBUG"

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
