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
cache_path = "cache/"
# output directory path
output_dir = "results/"
statistics_dir = os.path.join(output_dir, "statistics")
# path to the log file
log_path = os.path.join(output_dir, "log.txt")
# number of runs each
number_of_runs = 1
measure_interval_in_seconds = 15
# build and compile configurations
releaseOrDebug = "Release"
NES_LOG_LEVEL = "LEVEL_NONE"
log_level = "LOG_NONE"


PRELOG = "numactl -N 0 -m 0"


############## config params ##############
QUERIES = [
    os.path.join(
        args.project_dir, "window-patterns", "config", "joinTumbling50Config.yaml"
    ),
    os.path.join(
        args.project_dir, "window-patterns", "config", "joinTumbling100Config.yaml"
    ),
    # os.path.join(
    #     args.project_dir, "window-patterns", "config", "joinSlidingConfig.yaml"
    # ),
]

BUFFER_SIZE_PARAMS = [100000, 48000, 16000]

WORKER_THREADS_PARAMS = [8, 4, 2, 1]

UNORDEREDNESS_PARAMS = [
    {"delay_strategy": "BUFFER", "unorderedness": 0.0, "min_delay": 0, "max_delay": 0},
    {"delay_strategy": "TUPLES", "unorderedness": 1.0, "min_delay": 0, "max_delay": 0},
    # {
    #     "delay_strategy": "BUFFER",
    #     "unorderedness": 0.1,
    #     "min_delay": 1,
    #     "max_delay": 5,
    # },
    # {
    #     "delay_strategy": "BUFFER",
    #     "unorderedness": 0.25,
    #     "min_delay": 1,
    #     "max_delay": 1,
    # },
    # {
    #     "delay_strategy": "BUFFER",
    #     "unorderedness": 0.5,
    #     "min_delay": 1,
    #     "max_delay": 1,
    # },
    # {
    #     "delay_strategy": "BUFFER",
    #     "unorderedness": 0.75,
    #     "min_delay": 100,
    #     "max_delay": 500,
    # },
]

SLICE_STORE_PARAMS = ["MAP", "LIST"]

SLICE_CACHE_PARAMS = [
    {"slice_cache_type": "DEFAULT", "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 5, "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 10, "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 20, "lock_slice_cache": "false"},
    {"slice_cache_type": "LRU", "slice_cache_size": 5, "lock_slice_cache": "false"},
    {"slice_cache_type": "LRU", "slice_cache_size": 10, "lock_slice_cache": "false"},
    {"slice_cache_type": "LRU", "slice_cache_size": 20, "lock_slice_cache": "false"},
    {"slice_cache_type": "FIFO", "slice_cache_size": 10, "lock_slice_cache": "true"},
    {"slice_cache_type": "LRU", "slice_cache_size": 10, "lock_slice_cache": "true"},
]

SORTING_PARAMS = [
    # {"sort_buffer_by_field": ""},
    {"sort_buffer_by_field": "timestamp"},
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
}


############## variables for statistics ##############
original_statistics_path = "statistics.txt"
statistics_csv = os.path.join(output_dir, "statistics.csv")

csv_fieldnames = [
    "experimentID",
    "benchmarkName",
    "unorderedness",
    "bufferSizeInBytes",
    "numberOfWorkerThreads",
    "processedTuples",
    "processedTasks",
    "processedBuffers",
    "totaltimeInSeconds",
    "tuplesPerSecond",
    "tasksPerSecond",
    "buffersPerSecond",
    "mebiBPerSecond",
    "latencySumInMs",
    "avgLatencyPerTask",
    "avgThroughputPerTask",
    "avgThroughput200Tasks",
    "avgThroughput10Seconds",
    "cacheHits",
    "cacheMisses",
    "delayTasks",
    "totalDelayLatency",
    "sortBufferTasks",
    "totalSortBufferLatency",
    "totalLatencyAllPipelines",
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

    single_node_params = list(
        itertools.product(BUFFER_SIZE_PARAMS, WORKER_THREADS_PARAMS)
    )
    for query in QUERIES:
        current_params = dict(static_params)
        current_params["query"] = os.path.basename(query).replace("Config.yaml", "")
        for single_node_config in single_node_params:
            arguments = {
                "buffer_size": single_node_config[0],
                "worker_threads": single_node_config[1],
            }
            current_params.update(arguments)
            for slice_store_type in SLICE_STORE_PARAMS:
                current_params["slice_store_type"] = slice_store_type
                for unorderedness_config in UNORDEREDNESS_PARAMS:
                    if (
                        current_params["worker_threads"] == 1
                        and unorderedness_config["delay_strategy"] == "BUFFER"
                        and unorderedness_config["unorderedness"] != 0.0
                    ):
                        continue
                    current_params.update(**unorderedness_config)
                    # generate source name and benchmark name
                    current_params["unorderedness"] = (
                        unorderedness_config["unorderedness"] * 100
                    )
                    for slice_cache_config in SLICE_CACHE_PARAMS:
                        if (
                            current_params["worker_threads"] == 1
                            and slice_cache_config["lock_slice_cache"] == "true"
                        ):
                            continue
                        current_params["experiment_id"] = experiment_id
                        current_params.update(**slice_cache_config)
                        # generate slice cache name for benchmark name
                        slice_cache_name = slice_cache_config["slice_cache_type"]
                        if "slice_cache_size" in slice_cache_config:
                            slice_cache_name += str(
                                slice_cache_config["slice_cache_size"]
                            )
                        if slice_cache_config["lock_slice_cache"] == "true":
                            slice_cache_name = "Global" + slice_cache_name
                        current_params["benchmark_name"] = (
                            slice_store_type + slice_cache_name
                        )
                        # make sure sorting isn't used
                        current_params.pop("sort_buffer_by_field", None)

                        # start experiment
                        start_experiment(current_params, query, experiment_id)
                        experiment_id += 1
                    for sort_buffer_config in SORTING_PARAMS:
                        if (
                            sort_buffer_config["sort_buffer_by_field"] != ""
                            and unorderedness_config["delay_strategy"] == "BUFFER"
                            and unorderedness_config["unorderedness"] != 0.0
                        ):
                            continue
                        current_params["experiment_id"] = experiment_id
                        current_params.update(**sort_buffer_config)
                        current_params["benchmark_name"] = slice_store_type + "Sorted"
                        # make sure slice cache isn't used
                        current_params.pop("slice_cache_type", None)
                        # start experiment
                        start_experiment(current_params, query, experiment_id)
                        experiment_id += 1


def start_experiment(current_params, query, experiment_id):
    experiment_dir = os.path.join(statistics_dir, f"experiment_{experiment_id}")
    if not os.path.exists(experiment_dir):
        os.makedirs(experiment_dir)

    print(f"Starting experiment with ID {experiment_id}!\n")

    # start tcp servers
    tcp_server_process = start_tcp_server(5010)
    server_processes = [tcp_server_process]
    tcp_server_2_process = start_tcp_server(5011)
    server_processes.append(tcp_server_2_process)

    # start single node
    single_node_process = start_single_node(current_params)

    # run queries
    query_id = launch_query(query, experiment_id)
    # confirm_execution_and_sleep(single_node_process)
    if query_id is not None:
        stop_query(query_id)

    # wait for input before processing data
    if args.no_execution:
        print("Now waiting for input.")
        input()

    # terminate processes
    terminate_processes(server_processes + [single_node_process])

    # create csv from statistics
    reset_statistic_params(current_params)
    read_statistics(original_statistics_path, current_params, experiment_id)
    calculate_and_write_to_csv(current_params)

    log_params(current_params)

    print(f"Experiment with ID {experiment_id} finished!\n")


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
        f"--worker.queryCompiler.delayStrategy={current_params['delay_strategy']}",
        f"--worker.queryCompiler.unorderedness={current_params['unorderedness']}",
        f"--worker.queryCompiler.minDelay={current_params['min_delay']}",
        f"--worker.queryCompiler.maxDelay={current_params['max_delay']}",
        f"--worker.queryCompiler.sliceStoreType={current_params['slice_store_type']}",
    ]

    if "slice_cache_type" in current_params:
        arguments.append(
            f"--worker.queryCompiler.sliceCacheType={current_params['slice_cache_type']}",
        )
        if "slice_cache_size" in current_params:
            arguments.append(
                f"--worker.queryCompiler.sliceCacheSize={current_params['slice_cache_size']}"
            )
            arguments.append(
                f"--worker.queryCompiler.lockSliceCache={current_params['lock_slice_cache']}",
            )

    if (
        "sort_buffer_by_field" in current_params
        and current_params["sort_buffer_by_field"] != ""
    ):
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


def start_tcp_server(port):
    env = os.environ.copy()

    arguments = [
        f"-p {port}",
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


def launch_query(query, experiment_id):
    print(f"Launching query")
    cmd = [
        # "sh",
        # "-c",
        f"cat {query} | sed 's#GENERATE#{output_dir}experiment_{experiment_id}/query_sink.csv#' | {nebuli_path} register -x -s localhost:8080",
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


def read_statistics(statistics_file, current_params, experiment_id):
    print("Now reading the statistics... ")

    # process cache stats
    for i in range(0, int(current_params["worker_threads"])):
        cache_file = os.path.join(cache_path, f"cache_{i}.txt")
        if os.path.exists(cache_file):
            with open(cache_file, "r") as file:
                for line in file:
                    current_params["processed_buffers"] += 1
                    parts = line.split(" ")
                    if len(parts) > 1:
                        current_params["cache_hits"] += int(parts[1].strip())
                        current_params["cache_misses"] += int(parts[3].strip())
            # move file to output directory
            shutil.move(
                cache_file,
                os.path.join(
                    statistics_dir,
                    f"experiment_{experiment_id}",
                    os.path.basename(cache_file),
                ),
            )

    # extract relevant pipeline ids
    pipeline_ids = []
    delay_pipeline_ids = []
    sort_buffer_pipeline_ids = []
    if os.path.exists(pipelines_file):
        with open(pipelines_file, "r") as file:
            for line in file:
                parts = line.split("pipelineId: ")
                if len(parts) > 1:
                    p_id = int(parts[1].strip())
                    if "StreamJoinBuild" in line:
                        pipeline_ids.append(p_id)
                    elif "Delay" in line:
                        delay_pipeline_ids.append(p_id)
                    elif "SortBuffer" in line:
                        sort_buffer_pipeline_ids.append(p_id)
        # move file to output directory
        shutil.move(
            pipelines_file,
            os.path.join(
                statistics_dir,
                f"experiment_{experiment_id}",
                os.path.basename(pipelines_file).replace(
                    ".txt", f"_{experiment_id}.txt"
                ),
            ),
        )
    current_params["pipeline_ids"] = pipeline_ids

    if os.path.exists(statistics_file):
        with open(statistics_file, "r") as file:
            start_times = {}
            end_times = {}
            processed_tuples = {}
            completed_tasks = {}  # dict of sets
            latencies = {}
            task_throughputs = {}
            # For individual statistics
            first_match = False
            start_ts = datetime.now()
            measure_interval_end_ts = start_ts + timedelta(
                seconds=measure_interval_in_seconds
            )
            ten_secs_ts = start_ts + timedelta(seconds=10)
            ten_secs_reached = False
            task_count = 0
            task_count_reached = False

            # parse each line of the statistics file
            for line in file:
                start_match = start_pattern.match(line)
                end_match = end_pattern.match(line)
                if start_match:
                    start_timestamp = datetime.strptime(
                        start_match.group("timestamp"),
                        "%Y-%m-%d %H:%M:%S.%f",
                    )
                    if not first_match:
                        start_ts = start_timestamp  # + timedelta(seconds=1)
                        measure_interval_end_ts = start_ts + timedelta(
                            seconds=measure_interval_in_seconds
                        )
                        ten_secs_ts = start_ts + timedelta(seconds=10)
                        first_match = True
                    p_id = int(start_match.group("pipeline_id"))
                    t_id = int(start_match.group("task_id"))
                    if p_id not in start_times:
                        start_times[p_id] = dict()
                    if p_id not in processed_tuples:
                        processed_tuples[p_id] = dict()
                    start_times[p_id][t_id] = start_timestamp
                    processed_tuples[p_id][t_id] = int(start_match.group("tuples"))
                elif end_match:
                    end_timestamp = datetime.strptime(
                        end_match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f"
                    )
                    measure_interval_end_ts = end_timestamp
                    p_id = int(end_match.group("pipeline_id"))
                    t_id = int(end_match.group("task_id"))
                    if p_id in start_times and t_id in start_times[p_id]:
                        if p_id not in completed_tasks:
                            completed_tasks[p_id] = set()
                        completed_tasks[p_id].add(t_id)
                        if p_id not in end_times:
                            end_times[p_id] = dict()
                        end_times[p_id][t_id] = end_timestamp
                        difference = (
                            end_timestamp - start_times[p_id][t_id]
                        ).total_seconds()
                        if p_id not in latencies:
                            latencies[p_id] = dict()
                        latencies[p_id][t_id] = difference
                        if p_id not in task_throughputs:
                            task_throughputs[p_id] = dict()
                        task_throughputs[p_id][t_id] = 0
                        if difference != 0:
                            task_throughputs[p_id][t_id] = (
                                processed_tuples[p_id][t_id] / difference
                            )

                        if task_count_reached is False and p_id in pipeline_ids:
                            task_count += 1

                            if task_count >= 200:
                                current_params["avg_throughput_200"] = sum(
                                    t_id
                                    for p_id in pipeline_ids
                                    if p_id in task_throughputs
                                    for t_id in task_throughputs[p_id].values()
                                ) / sum(
                                    len(completed_tasks[p_id])
                                    for p_id in pipeline_ids
                                    if p_id in completed_tasks
                                )
                                task_count_reached = True
                        if ten_secs_reached is False and p_id in pipeline_ids:
                            if end_timestamp >= ten_secs_ts:
                                current_params["avg_throughput_10_secs"] = (
                                    sum(
                                        t_id
                                        for p_id in pipeline_ids
                                        if p_id in processed_tuples
                                        for t_id in processed_tuples[p_id].values()
                                    )
                                    / (ten_secs_ts - start_ts).total_seconds()
                                )
                                ten_secs_reached = True

            current_params["processed_tuples"] = sum(
                t_id
                for p_id in pipeline_ids
                if p_id in processed_tuples
                for t_id in processed_tuples[p_id].values()
            )
            current_params["processed_tasks"] = sum(
                len(completed_tasks[p_id])
                for p_id in pipeline_ids
                if p_id in completed_tasks
            )
            current_params["time_in_seconds"] = (
                measure_interval_end_ts - start_ts
            ).total_seconds()
            current_params["latency_in_secs"] = sum(
                t_id
                for p_id in pipeline_ids
                if p_id in latencies
                for t_id in latencies[p_id].values()
            )
            current_params["total_latency_in_secs"] = sum(
                t_id for p_dict in latencies.values() for t_id in p_dict.values()
            )
            current_params["delay_latency_in_secs"] = sum(
                t_id
                for p_id in delay_pipeline_ids
                if p_id in latencies
                for t_id in latencies[p_id].values()
            )
            current_params["delay_tasks"] = sum(
                len(completed_tasks[p_id])
                for p_id in delay_pipeline_ids
                if p_id in completed_tasks
            )
            current_params["sort_buffer_latency_in_secs"] = sum(
                t_id
                for p_id in sort_buffer_pipeline_ids
                if p_id in latencies
                for t_id in latencies[p_id].values()
            )
            current_params["sort_buffer_tasks"] = sum(
                len(completed_tasks[p_id])
                for p_id in sort_buffer_pipeline_ids
                if p_id in completed_tasks
            )
            if current_params["processed_tasks"] != 0:
                current_params["avg_latency_per_task"] = (
                    current_params["latency_in_secs"] * 1000
                ) / current_params["processed_tasks"]
                current_params["avg_throughput_per_task"] = (
                    sum(
                        t_id
                        for p_id in pipeline_ids
                        if p_id in task_throughputs
                        for t_id in task_throughputs[p_id].values()
                    )
                    / current_params["processed_tasks"]
                )

            # save individual task statistics
            task_statistics_path = os.path.join(
                statistics_dir,
                f"experiment_{experiment_id}",
                f"statistics_{experiment_id}.csv",
            )
            write_individual_task_statistics(
                task_statistics_path,
                pipeline_ids,
                completed_tasks,
                start_times,
                end_times,
                processed_tuples,
                latencies,
                task_throughputs,
            )
            delay_task_statistics_path = os.path.join(
                statistics_dir,
                f"experiment_{experiment_id}",
                f"delay_statistics_{experiment_id}.csv",
            )
            write_individual_task_statistics(
                delay_task_statistics_path,
                delay_pipeline_ids,
                completed_tasks,
                start_times,
                end_times,
                processed_tuples,
                latencies,
                task_throughputs,
            )
            if "sort_buffer_by_field" in current_params:
                sort_buffer_task_statistics_path = os.path.join(
                    statistics_dir,
                    f"experiment_{experiment_id}",
                    f"sort_buffer_statistics_{experiment_id}.csv",
                )
                write_individual_task_statistics(
                    sort_buffer_task_statistics_path,
                    delay_pipeline_ids,
                    completed_tasks,
                    start_times,
                    end_times,
                    processed_tuples,
                    latencies,
                    task_throughputs,
                )

        # move file to output directory
        shutil.move(
            statistics_file,
            os.path.join(
                statistics_dir,
                f"experiment_{experiment_id}",
                os.path.basename(statistics_file).replace(
                    ".txt", f"_{experiment_id}.txt"
                ),
            ),
        )


def write_individual_task_statistics(
    task_statistics_path,
    pipeline_ids,
    completed_tasks,
    start_times,
    end_times,
    processed_tuples,
    latencies,
    task_throughputs,
):
    with open(task_statistics_path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=task_statistics_fieldnames)
        writer.writeheader()

        # filter relavant tasks and sort by task id
        relevant_tasks = [
            (p_id, t_id)
            for p_id in pipeline_ids
            if p_id in completed_tasks
            for t_id in completed_tasks[p_id]
        ]
        sorted_tasks = sorted(relevant_tasks, key=lambda x: x[1])
        for p_id, t_id in sorted_tasks:
            writer.writerow(
                {
                    "taskId": t_id,
                    "pipelineId": p_id,
                    "startTime": start_times[p_id][t_id],
                    "endTime": end_times[p_id][t_id],
                    "processedTuples": processed_tuples[p_id][t_id],
                    "latencyInMs": latencies[p_id][t_id] * 1000,
                    "tuplesPerSecond": task_throughputs[p_id][t_id],
                }
            )


def calculate_and_write_to_csv(current_params):
    print(f"Writing statistics to {statistics_csv}\n")
    with open(statistics_csv, "a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)

        # calculate latency and throughput
        tuples = current_params["processed_tuples"]
        total_time = current_params["time_in_seconds"]
        throughput = 0
        if tuples != 0 and total_time != 0:
            throughput = tuples / total_time  # throughput in tuples/s

        # calculate tasks and buffers per second
        tasks = current_params["processed_tasks"]
        tasks_per_second = 0
        if tasks != 0 and total_time != 0:
            tasks_per_second = tasks / total_time

        # buffer_size_in_tuples = current_params["buffer_size"] / current_params["schema_size"]
        buffers = current_params["processed_buffers"]  # tuples / buffer_size_in_tuples
        buffers_per_second = 0
        if buffers != 0 and total_time != 0:
            buffers_per_second = buffers / total_time

        mebi_per_second = (throughput * 32) / (1024 * 1024)

        writer.writerow(
            {
                "experimentID": current_params["experiment_id"],
                "benchmarkName": current_params["benchmark_name"],
                "unorderedness": current_params["unorderedness"],
                "bufferSizeInBytes": current_params["buffer_size"],
                "numberOfWorkerThreads": current_params["worker_threads"],
                "processedTuples": tuples,
                "processedTasks": tasks,
                "processedBuffers": buffers,
                "totaltimeInSeconds": total_time,
                "tuplesPerSecond": throughput,
                "tasksPerSecond": tasks_per_second,
                "buffersPerSecond": buffers_per_second,
                "mebiBPerSecond": mebi_per_second,
                "latencySumInMs": current_params["latency_in_secs"] * 1000,
                "avgLatencyPerTask": current_params["avg_latency_per_task"],
                "avgThroughputPerTask": current_params["avg_throughput_per_task"],
                "avgThroughput200Tasks": current_params["avg_throughput_200"],
                "avgThroughput10Seconds": current_params["avg_throughput_10_secs"],
                "cacheHits": current_params["cache_hits"],
                "cacheMisses": current_params["cache_misses"],
                "delayTasks": current_params["delay_tasks"],
                "totalDelayLatency": current_params["delay_latency_in_secs"] * 1000,
                "sortBufferTasks": current_params["sort_buffer_tasks"],
                "totalSortBufferLatency": current_params["sort_buffer_latency_in_secs"]
                * 1000,
                "totalLatencyAllPipelines": current_params["total_latency_in_secs"]
                * 1000,
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
    current_params["processed_tuples"] = 0
    current_params["processed_tasks"] = 0
    current_params["processed_buffers"] = 0
    current_params["latency_in_secs"] = 0
    current_params["total_latency_in_secs"] = 0
    current_params["avg_latency_per_task"] = 0
    current_params["avg_throughput_per_task"] = 0
    current_params["avg_throughput_200"] = 0
    current_params["avg_throughput_10_secs"] = 0
    current_params["cache_hits"] = 0
    current_params["cache_misses"] = 0
    current_params["delay_tasks"] = 0
    current_params["delay_latency_in_secs"] = 0
    current_params["sort_buffer_tasks"] = 0
    current_params["sort_buffer_latency_in_secs"] = 0
    current_params["time_in_seconds"] = measure_interval_in_seconds
    current_params["pipeline_ids"] = []


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
        os.makedirs(statistics_dir)

    # create statistics csv
    with open(statistics_csv, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        writer.writeheader()

    run_benchmark(number_of_runs)

    print(f"Benchmarks done. See results in {output_dir}.")


if __name__ == "__main__":
    main()
