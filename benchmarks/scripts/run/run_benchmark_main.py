#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
import os
import shutil
import subprocess
import time
import math
import random
import datetime
import yaml
import pathlib
import copy
import pandas as pd
import BenchmarkConfig
import PostProcessing


# Configuration for compilation
BUILD_PROJECT = False
BUILD_DIR = "cmake-build-relnologging"
# SOURCE_DIR = "/home/nikla/Documents/Nebulastream/nebulastream-1"
SOURCE_DIR = "/home/ntantow/Documents/NebulaStream/nebulastream-public_1"
CMAKE_TOOLCHAIN_FILE = "/home/ntantow/Documents/NebulaStream/vcpkg/scripts/buildsystems/vcpkg.cmake"
LOG_LEVEL = "LEVEL_NONE"
CMAKE_BUILD_TYPE = "Release"
USE_LIBCXX_IF_AVAILABLE = "False"  # False for using libstdcxx, True for libcxx
NEBULI_PATH = os.path.join(SOURCE_DIR, BUILD_DIR, "nes-nebuli/nes-nebuli")
SINGLE_NODE_PATH = os.path.join(SOURCE_DIR, BUILD_DIR, "nes-single-node-worker/nes-single-node-worker")
TCP_SERVER = os.path.join(SOURCE_DIR, BUILD_DIR, "benchmarks/tcpserver")

# Configuration for benchmark run
NUM_RUNS_PER_CONFIG = 3
NUM_RETRIES_PER_RUN = 3
WAIT_BEFORE_SIGKILL = 5
WAIT_BEFORE_QUERY_STOP = 5
MEASURE_INTERVAL = 8
WAIT_BETWEEN_COMMANDS = 2

# Compilation for misc.
SERVER_NAME = "amd"
DESTINATION_PATH = os.path.join("/home/ntantow/Downloads/ba-benchmark/", SERVER_NAME)
DATETIME_NOW = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
RESULTS_DIR = f"benchmarks/data/{DATETIME_NOW}"
WORKING_DIR = f".cache/benchmarks/{DATETIME_NOW}"
COMBINED_ENGINE_STATISTICS_FILE = "combined_engine_statistics.csv"
COMBINED_BENCHMARK_STATISTICS_FILE = "combined_benchmark_statistics.csv"
BENCHMARK_STATS_FILE = "BenchmarkStats_"
ENGINE_STATS_FILE = "EngineStats_"
PIPELINE_TXT = "pipelines.txt"
WORKER_CONFIG = "worker"
QUERY_CONFIG = "query"
BENCHMARK_CONFIG_FILE = "benchmark_config.yaml"
WORKER_CONFIG_FILE_NAME = "worker_config.yaml"
QUERY_CONFIG_FILE_NAME = "query.yaml"
CONFIG_FILES = {
    WORKER_CONFIG: os.path.join(pathlib.Path(__file__).parent.resolve(), "configs", WORKER_CONFIG_FILE_NAME),
    QUERY_CONFIG: os.path.join(pathlib.Path(__file__).parent.resolve(), "configs", QUERY_CONFIG_FILE_NAME),
}


# Helper functions
def clear_build_dir():
    if os.path.exists(BUILD_DIR):
        shutil.rmtree(BUILD_DIR)
    os.mkdir(BUILD_DIR)


def compile_project():
    cmd_cmake = ["cmake",
                 f"-DCMAKE_BUILD_TYPE={CMAKE_BUILD_TYPE}",
                 f"-DNES_LOG_LEVEL={LOG_LEVEL}",
                 f"-DCMAKE_TOOLCHAIN_FILE={CMAKE_TOOLCHAIN_FILE}",
                 f"-DUSE_LIBCXX_IF_AVAILABLE:BOOL={USE_LIBCXX_IF_AVAILABLE}",
                 f"-S {SOURCE_DIR}",
                 f"-B {BUILD_DIR}",
                 "-G Ninja"]
    cmd_build = f"cmake --build {BUILD_DIR} -j -- -k 0".split(" ")
    subprocess.run(cmd_cmake, check=True)
    subprocess.run(cmd_build, check=True)


def create_results_dir():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    # print(f"Created results dir {folder_name}...")
    return [os.path.join(RESULTS_DIR, COMBINED_ENGINE_STATISTICS_FILE),
            os.path.join(RESULTS_DIR, COMBINED_BENCHMARK_STATISTICS_FILE)]


def create_working_dir(output_folder):
    folder_name = os.path.join(output_folder, "working_dir")
    os.makedirs(folder_name, exist_ok=True)
    # print(f"Created working dir {folder_name}...")
    return folder_name


def create_output_folder():
    timestamp = int(time.time())
    folder_name = os.path.join(WORKING_DIR, f"SpillingBenchmarks_{timestamp}")
    os.makedirs(folder_name, exist_ok=True)
    print(f"Created folder {folder_name}...")
    return folder_name


def calculate_id_upper_bound_for_match_rate(current_benchmark_config):
    window_pattern = r"WINDOW SLIDING \(timestamp, size (\d+) ms, advance by (\d+) ms\)"
    size_and_slide = re.search(window_pattern, current_benchmark_config.query)
    window_size = int(size_and_slide.group(1))
    timestamp_increment = current_benchmark_config.timestamp_increment
    match_rate = current_benchmark_config.match_rate

    if match_rate <= 0:
        return int(np.iinfo(np.int64).max)  # Practically infinite, no matches expected
    elif match_rate >= 100:
        return 1  # Smallest useful range which ensures very high collisions

    num_tuples = window_size / timestamp_increment
    upper_bound = (num_tuples / (match_rate / 100)) - 1
    return max(1, math.ceil(upper_bound))  # Prevent upper_bound < 1


def copy_and_modify_configs(output_folder, working_dir, current_benchmark_config, tcp_server_ports):
    # Creating a dest path for the worker and query config yaml file
    dest_path_worker = os.path.join(output_folder, WORKER_CONFIG_FILE_NAME)
    dest_path_query = os.path.join(output_folder, QUERY_CONFIG_FILE_NAME)

    # Worker Configuration
    with open(CONFIG_FILES[WORKER_CONFIG], 'r') as input_file:
        worker_config_yaml = yaml.safe_load(input_file)
    worker_config_yaml["worker"][
        "numberOfBuffersInGlobalBufferManager"] = current_benchmark_config.buffers_in_global_buffer_manager
    worker_config_yaml["worker"]["numberOfBuffersPerWorker"] = current_benchmark_config.buffers_per_worker
    worker_config_yaml["worker"]["bufferSizeInBytes"] = current_benchmark_config.buffer_size_in_bytes
    worker_config_yaml["worker"][
        "throughputListenerTimeInterval"] = current_benchmark_config.throughput_listener_time_interval

    # Query Optimizer Configuration
    worker_config_yaml["worker"]["queryOptimizer"]["executionMode"] = current_benchmark_config.execution_mode
    worker_config_yaml["worker"]["queryOptimizer"]["pipelinesTxtFilePath"] = os.path.abspath(
        os.path.join(output_folder, PIPELINE_TXT))
    worker_config_yaml["worker"]["queryOptimizer"]["pageSize"] = current_benchmark_config.page_size
    worker_config_yaml["worker"]["queryOptimizer"]["sliceStoreType"] = current_benchmark_config.slice_store_type
    worker_config_yaml["worker"]["queryOptimizer"]["lowerMemoryBound"] = current_benchmark_config.lower_memory_bound
    worker_config_yaml["worker"]["queryOptimizer"]["upperMemoryBound"] = current_benchmark_config.upper_memory_bound
    worker_config_yaml["worker"]["queryOptimizer"][
        "fileDescriptorBufferSize"] = current_benchmark_config.file_descriptor_buffer_size
    worker_config_yaml["worker"]["queryOptimizer"][
        "maxNumWatermarkGaps"] = current_benchmark_config.max_num_watermark_gaps
    worker_config_yaml["worker"]["queryOptimizer"][
        "maxNumSequenceNumbers"] = current_benchmark_config.max_num_sequence_numbers
    worker_config_yaml["worker"]["queryOptimizer"]["minReadStateSize"] = current_benchmark_config.min_read_state_size
    worker_config_yaml["worker"]["queryOptimizer"][
        "minWriteStateSize"] = current_benchmark_config.min_write_state_size
    worker_config_yaml["worker"]["queryOptimizer"][
        "fileOperationTimeDelta"] = current_benchmark_config.file_operation_time_delta
    worker_config_yaml["worker"]["queryOptimizer"]["fileLayout"] = current_benchmark_config.file_layout
    worker_config_yaml["worker"]["queryOptimizer"][
        "watermarkPredictorType"] = current_benchmark_config.watermark_predictor_type
    worker_config_yaml["worker"]["queryOptimizer"]["fileBackedWorkingDir"] = working_dir

    # Query Engine Configuration
    worker_config_yaml["worker"]["queryEngine"][
        "numberOfWorkerThreads"] = current_benchmark_config.number_of_worker_threads
    worker_config_yaml["worker"]["queryEngine"]["taskQueueSize"] = current_benchmark_config.task_queue_size
    # TODO worker_config_yaml["worker"]["queryEngine"]["statisticsDir"] = os.path.abspath(output_folder)

    # Dumping the updated worker config yaml to the dest path
    with open(dest_path_worker, 'w') as output_file:
        yaml.dump(worker_config_yaml, output_file)

    # Changing the query
    with open(CONFIG_FILES[QUERY_CONFIG], 'r') as input_file:
        query_config_yaml = yaml.safe_load(input_file)
    query_config_yaml["query"] = current_benchmark_config.query

    # Change the csv sink file
    query_config_yaml["sink"]["config"]["filePath"] = os.path.abspath(os.path.join(output_folder, "csv_sink.csv"))

    # Duplicating the physical sources until we have the same number of physical sources as configured in the benchmark config
    assert len(tcp_server_ports) % len(query_config_yaml["physical"]) == 0, \
        "The number of physical sources must be a multiple of the number of TCP server ports."

    # Iterating over the physical sources and writing as many in a separate list as we have configured in the benchmark config
    new_physical_sources = []
    for idx, physical_source in enumerate(query_config_yaml["physical"]):
        for i in range(current_benchmark_config.no_physical_sources_per_logical_source):
            new_physical_sources.append(copy.deepcopy(physical_source))

    # Replacing the old physical sources with the new ones
    query_config_yaml["physical"] = new_physical_sources

    # Changing the source ports
    for idx, port in enumerate(tcp_server_ports):
        query_config_yaml["physical"][idx]["sourceConfig"]["socketPort"] = int(port)

    # Dumping the changed query config yaml to the dest path
    with open(dest_path_query, 'w') as output_file:
        yaml.dump(query_config_yaml, output_file)

    # Write the current benchmark config to the output folder in a way that it can be easily read
    # We use a dictionary representation of the configuration
    # Convert all values in the dictionary to strings to retain precision
    config_values_as_string = {key: str(value) for key, value in current_benchmark_config.to_dict().items()}
    with open(os.path.join(output_folder, BENCHMARK_CONFIG_FILE), 'w') as output_file:
        yaml.dump(config_values_as_string, output_file)


def start_tcp_servers(starting_ports, current_benchmark_config):
    processes = []
    ports = []
    max_retries = 10
    generator_seed = 42
    for j, port in enumerate(starting_ports):
        for i in range(benchmark_config.no_physical_sources_per_logical_source):
            for attempt in range(max_retries):
                remainingServers = len(starting_ports) - j
                cmd = f"{TCP_SERVER} -p {port} " \
                      f"-t {MEASURE_INTERVAL + remainingServers * WAIT_BETWEEN_COMMANDS + 2 * WAIT_BETWEEN_COMMANDS} " \
                      f"-n {current_benchmark_config.num_tuples_to_generate} " \
                      f"-s {current_benchmark_config.timestamp_increment} " \
                      f"-i {current_benchmark_config.ingestion_rate} " \
                      f"-b {calculate_id_upper_bound_for_match_rate(current_benchmark_config)} " \
                      f"-g {generator_seed + j}"
                # Disregard uniform int distribution and achieve true 100% match rate
                if current_benchmark_config.match_rate >= 100:
                    cmd += " -d"
                # Add variable sized data flag to last two tcp servers
                if port == starting_ports[-1] or port == starting_ports[-2]:
                    cmd += " -v"
                # print(f"Trying to start tcp server with {cmd}")
                process = subprocess.Popen(cmd.split(" "), stdout=subprocess.DEVNULL)
                time.sleep(WAIT_BETWEEN_COMMANDS)  # Allow server to start
                if process.poll() is not None and process.poll() != 0:
                    # print(f"Failed to start tcp server with PID: {process.pid} and port: {port}")
                    port = str(int(port) + random.randint(1, 10))
                    terminate_process_if_exists(process)
                    time.sleep(1)
                else:
                    # print(f"Started tcp server with PID: {process.pid} and port: {port}")
                    processes.append(process)
                    ports.append(port)
                    port = str(int(port) + 1)  # Increment the port for the next server
                    break
            else:
                raise Exception(f"Failed to start the TCP server after {max_retries} attempts.")

    print(
        f"Started all TCP servers with the following <port, pid>: {list(zip(ports, [proc.pid for proc in processes]))}")
    return [processes, ports]


def submitting_query(query_file):
    cmd = f"cat {query_file} | {NEBULI_PATH} register -x -s localhost:8080"
    # print(f"Submitting the query via {cmd}...")
    # shell=True is needed to pipe the output of cat to the register command
    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # print(f"Submitted the query with the following output: {result.stdout.strip()} and error: {result.stderr.strip()}")
    stdout, stderr = process.communicate()
    query_id = stdout.strip()
    print(f"Submitted the query with id {query_id}")
    return query_id


def start_single_node_worker(worker_config_file):
    cmd = f"{SINGLE_NODE_PATH} --configPath={worker_config_file}"
    # print(f"Starting the single node worker with {cmd}")
    process = subprocess.Popen(cmd.split(" "), stdout=subprocess.DEVNULL)
    pid = process.pid
    print(f"Started single node worker with pid {pid}")
    return process


def stop_query(query_id):
    cmd = f"{NEBULI_PATH} stop {query_id} -s localhost:8080"
    # print(f"Stopping the query via {cmd}...")
    process = subprocess.Popen(cmd.split(" "), stdout=subprocess.DEVNULL)
    return process


def terminate_process_if_exists(process):
    try:
        process.terminate()
        process.wait(timeout=5)
        print(f"Process with PID {process.pid} terminated.")
    except subprocess.TimeoutExpired:
        print(f"Process with PID {process.pid} did not terminate within timeout. Sending SIGKILL.")
        process.kill()
        process.wait()
        print(f"Process with PID {process.pid} forcefully killed.")


def get_start_ports():
    # Extract all socketPort values
    with open(CONFIG_FILES[QUERY_CONFIG], 'r') as input_file:
        query_config_yaml = yaml.safe_load(input_file)
    source_ports = [
        item['sourceConfig']['socketPort']
        for item in query_config_yaml['physical']
        if 'sourceConfig' in item and 'socketPort' in item['sourceConfig']
    ]
    source_ports = [str(port) for port in source_ports]
    # print(f"Got the following start source ports: {' '.join(source_ports)}")
    return source_ports


# Benchmark loop
def run_benchmark(current_benchmark_config):
    # Defining here two empty lists so that we can use it in the finally
    output_folder = ""
    working_dir = ""
    source_processes = []
    single_node_process = []
    stop_process = []
    try:
        # Starting the TCP servers
        [source_processes, tcp_server_ports] = start_tcp_servers(get_start_ports(), current_benchmark_config)

        # Creating a new output folder and updating the configs with the current benchmark configs
        output_folder = create_output_folder()
        working_dir = create_working_dir(output_folder)
        copy_and_modify_configs(output_folder, working_dir, current_benchmark_config, tcp_server_ports)

        # Waiting before starting the single node worker
        time.sleep(WAIT_BETWEEN_COMMANDS)

        # Starting the single node worker
        single_node_process = start_single_node_worker(
            os.path.abspath(os.path.join(output_folder, WORKER_CONFIG_FILE_NAME)))

        # Waiting before submitting the query
        time.sleep(WAIT_BETWEEN_COMMANDS)

        # Submitting the query and waiting couple of seconds before stopping the query
        query_id = submitting_query(os.path.abspath(os.path.join(output_folder, QUERY_CONFIG_FILE_NAME)))

        print(f"Waiting for {MEASURE_INTERVAL + WAIT_BEFORE_QUERY_STOP}s before stopping the query...")
        time.sleep(MEASURE_INTERVAL + WAIT_BEFORE_QUERY_STOP)  # Allow query engine stats to be printed
        stop_process = stop_query(query_id)
        print(f"Query {query_id} was stopped")
    finally:
        time.sleep(WAIT_BEFORE_SIGKILL)  # Wait additional time before cleanup
        all_processes = source_processes + [single_node_process] + [stop_process]
        for proc in all_processes:
            terminate_process_if_exists(proc)

        # Move logs and statistics to output folder
        for file_name in os.listdir(os.getcwd()):
            if file_name.startswith(ENGINE_STATS_FILE) or file_name.startswith(
                    BENCHMARK_STATS_FILE) or file_name.endswith(".log"):
                source_file = os.path.join(os.getcwd(), file_name)
                shutil.move(source_file, output_folder)

        # Delete working directory
        print("Deleting working directory...\n")
        shutil.rmtree(working_dir)

        return output_folder


def load_csv(file_path):
    if os.path.getsize(file_path) == 0:  # Check if the file is empty
        return pd.DataFrame()  # Return an empty DataFrame
    return pd.read_csv(file_path)


def get_directory_size(directory):
    total_size = 0
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            total_size += os.path.getsize(filepath)
    return total_size


def format_data_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024


if __name__ == "__main__":
    if BUILD_PROJECT:
        # Removing the build folder and compiling the project
        clear_build_dir()
        compile_project()

    ALL_BENCHMARK_CONFIGS = BenchmarkConfig.create_benchmark_configs()

    for attempt in range(NUM_RETRIES_PER_RUN):
        num_runs_per_config = NUM_RUNS_PER_CONFIG if attempt == 0 else 1

        # Running all benchmarks
        output_folders = []
        iteration_times = []
        num_configs = len(ALL_BENCHMARK_CONFIGS)
        total_iterations = num_configs * num_runs_per_config
        print(f"Attempt {attempt + 1} of {NUM_RETRIES_PER_RUN} of running each of the {num_configs} experiments "
              f"{num_runs_per_config} times, totaling {total_iterations} runs...\n")

        for j in range(num_runs_per_config):
            for i, benchmark_config in enumerate(ALL_BENCHMARK_CONFIGS, start=(j * num_configs + 1)):
                start_time = time.time()

                output_folders.append(run_benchmark(benchmark_config))
                time.sleep(1)  # Sleep for a second to allow the system to clean up

                end_time = time.time()
                elapsed_time = end_time - start_time
                iteration_times.append(elapsed_time)

                # Calculate average time per iteration
                avg_time_per_iteration = sum(iteration_times) / len(iteration_times)

                # Estimate remaining time
                remaining_iterations = total_iterations - i
                remaining_time = remaining_iterations * avg_time_per_iteration
                eta = datetime.timedelta(seconds=int(remaining_time))

                # Calculate estimated finish time
                finish_time = datetime.datetime.now() + eta

                # Print ETA and finish time in cyan
                print(f"\033[96mIteration {i}/{total_iterations} completed. ETA: {eta}, "
                      f"Estimated Finish Time: {finish_time.strftime('%Y-%m-%d %H:%M:%S')}\033[0m\n")

        # Calling the postprocessing main
        measurement_time = MEASURE_INTERVAL * 1000
        startup_time = WAIT_BETWEEN_COMMANDS * 1000
        engine_stats_csv_path, benchmark_stats_csv_path = create_results_dir()
        post_processing = PostProcessing.PostProcessing(output_folders,
                                                        measurement_time,
                                                        startup_time,
                                                        BENCHMARK_CONFIG_FILE,
                                                        ENGINE_STATS_FILE,
                                                        BENCHMARK_STATS_FILE,
                                                        COMBINED_ENGINE_STATISTICS_FILE,
                                                        COMBINED_BENCHMARK_STATISTICS_FILE,
                                                        engine_stats_csv_path,
                                                        benchmark_stats_csv_path)
        failed_run_folders = post_processing.main()

        # Re-running all runs that failed
        if not failed_run_folders:
            print("All benchmarks completed successfully.")
            break
        else:
            print(f"{len(failed_run_folders)} runs failed. Retrying...")
            ALL_BENCHMARK_CONFIGS = [
                BenchmarkConfig.BenchmarkConfig(**{k: v for k, v in yaml.safe_load(
                    open(os.path.join(failed_run, BENCHMARK_CONFIG_FILE), 'r')).items() if
                        k not in {"task_queue_size", "buffers_in_global_buffer_manager", "buffers_per_worker",
                                  "buffers_in_source_local_buffer_pool", "execution_mode"}})
                for failed_run in failed_run_folders
            ]
    else:
        print(f"Maximum retries reached. Some runs still failed:\n{failed_run_folders}")

    results_path = os.path.join(SOURCE_DIR, RESULTS_DIR)
    copy_command = f"scp -r {SERVER_NAME}:{results_path} {DESTINATION_PATH}"
    print(f"\n\033[96mSize of results is {format_data_size(get_directory_size(results_path))}. "
          f"Copy with:\n{copy_command}\033[0m")

    # print("\nStarting post processing...\n")
    # # Compare the results of default slice store and file backed variant
    # file1 = f"/tmp/csv_sink_{1}.csv"
    # file2 = f"/tmp/csv_sink_{2}.csv"
    #
    # df1 = load_csv(file1)
    # df2 = load_csv(file2)
    #
    # # Check if either DataFrame is empty
    # if df1.empty and df2.empty:
    #     print("Both files are empty.")
    # elif df1.empty:
    #     print("File1 is empty.")
    #     print(f"File2 has {len(df2)} rows with keys: {list(df2['tcp_source4$id4:UINT64'])}")
    # elif df2.empty:
    #     print("File2 is empty.")
    #     print(f"File1 has {len(df1)} rows with keys: {list(df1['tcp_source4$id4:UINT64'])}")
    # else:
    #     # Set the key column as the index
    #     key_column = "tcp_source4$id4:UINT64"
    #     df1.set_index(key_column, inplace=True)
    #     df2.set_index(key_column, inplace=True)
    #
    #     # Align rows based on the key column
    #     common_keys = df1.index.intersection(df2.index)
    #     df1_aligned = df1.loc[common_keys]
    #     df2_aligned = df2.loc[common_keys]
    #
    #     # Compare rows and find differences
    #     differences = []
    #     for key in common_keys:
    #         if not df1_aligned.loc[key].equals(df2_aligned.loc[key]):
    #             differences.append((key, df1_aligned.loc[key].to_dict(), df2_aligned.loc[key].to_dict()))
    #
    #     # Print the differences
    #     if not differences:
    #         print("The rows with common keys are identical.")
    #     else:
    #         print("Differences found:")
    #         for diff in differences:
    #             print(f"Key {diff[0]} differs:")
    #             print(f"File1: {diff[1]}")
    #             print(f"File2: {diff[2]}")
    #
    #     # Identify additional keys in each file
    #     extra_keys_file1 = df1.index.difference(df2.index)
    #     extra_keys_file2 = df2.index.difference(df1.index)
    #
    #     if not extra_keys_file1.empty:
    #         print(f"File1 has {len(extra_keys_file1)} additional keys: {list(extra_keys_file1)}")
    #     if not extra_keys_file2.empty:
    #         print(f"File2 has {len(extra_keys_file2)} additional keys: {list(extra_keys_file2)}")
