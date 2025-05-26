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


import os
import shutil
import subprocess
import time
import random
import datetime
import yaml
import pathlib
import copy
import pandas as pd
import BenchmarkConfig


# Configuration for execution
BUILD_DIR = "cmake-build-relnologging"
SOURCE_DIR = "/home/nikla/Documents/Nebulastream/nebulastream-1"
# SOURCE_DIR = "/home/ntantow/Documents/NebulaStream/nebulastream-public_1"
NEBULI_PATH = os.path.join(SOURCE_DIR, BUILD_DIR, "nes-nebuli/nes-nebuli")
SINGLE_NODE_PATH = os.path.join(SOURCE_DIR, BUILD_DIR, "nes-single-node-worker/nes-single-node-worker")
TCP_SERVER = os.path.join(SOURCE_DIR, BUILD_DIR, "benchmarks/tcpserver")

# Configuration for benchmark run
WAIT_BEFORE_SIGKILL = 5
MEASURE_INTERVAL = 5
WAIT_BETWEEN_COMMANDS = 2

# Compilation for misc.
WORKING_DIR = f".cache/benchmarks/{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}"
WORKER_CONFIG = "worker"
QUERY_CONFIG = "query"
BENCHMARK_CONFIG_FILE = "benchmark_config.yaml"
WORKER_CONFIG_FILE_NAME = "worker_config.yaml"
QUERY_CONFIG_FILE_NAME = "query.yaml"
CONFIG_FILES = {
    WORKER_CONFIG: os.path.join(pathlib.Path(__file__).parent.resolve(), "configs", WORKER_CONFIG_FILE_NAME),
    QUERY_CONFIG: os.path.join(pathlib.Path(__file__).parent.resolve(), "configs", QUERY_CONFIG_FILE_NAME),
}


def create_working_dir():
    folder_name = os.path.join(WORKING_DIR, "working_dir")
    os.makedirs(folder_name, exist_ok=True)
    # print(f"Created working dir {folder_name}...")
    return folder_name


def create_output_folder():
    timestamp = int(time.time())
    folder_name = os.path.join(WORKING_DIR, f"SpillingBenchmarks_{timestamp}")
    os.makedirs(folder_name, exist_ok=True)
    print(f"Created folder {folder_name}...")
    return folder_name


def copy_and_modify_configs(output_folder, working_dir, current_benchmark_config, tcp_server_ports, iteration):
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
    worker_config_yaml["worker"]["queryOptimizer"]["pageSize"] = current_benchmark_config.page_size
    worker_config_yaml["worker"]["queryOptimizer"][
        "numWatermarkGapsAllowed"] = current_benchmark_config.num_watermark_gaps_allowed
    worker_config_yaml["worker"]["queryOptimizer"][
        "maxNumSequenceNumbers"] = current_benchmark_config.max_num_sequence_numbers
    worker_config_yaml["worker"]["queryOptimizer"][
        "fileDescriptorBufferSize"] = current_benchmark_config.file_descriptor_buffer_size
    worker_config_yaml["worker"]["queryOptimizer"]["minReadStateSize"] = current_benchmark_config.min_read_state_size
    worker_config_yaml["worker"]["queryOptimizer"][
        "minWriteStateSize"] = current_benchmark_config.min_write_state_size
    worker_config_yaml["worker"]["queryOptimizer"][
        "fileOperationTimeDelta"] = current_benchmark_config.file_operation_time_delta
    worker_config_yaml["worker"]["queryOptimizer"]["fileLayout"] = current_benchmark_config.file_layout
    worker_config_yaml["worker"]["queryOptimizer"][
        "watermarkPredictorType"] = current_benchmark_config.watermark_predictor_type
    worker_config_yaml["worker"]["queryOptimizer"]["sliceStoreType"] = current_benchmark_config.slice_store_type
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
    query_config_yaml["sink"]["config"]["filePath"] = f"/tmp/csv_sink_{iteration}.csv"

    # Duplicating the physical sources until we have the same number of physical sources as configured in the benchmark config
    assert len(tcp_server_ports) % len(query_config_yaml[
                                           "physical"]) == 0, "The number of physical sources must be a multiple of the number of TCP server ports."

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
    with open(os.path.join(output_folder, BENCHMARK_CONFIG_FILE), 'w') as output_file:
        yaml.dump(current_benchmark_config.to_dict(), output_file)


def start_tcp_servers(starting_ports, current_benchmark_config):
    processes = []
    ports = []
    max_retries = 10
    for port in starting_ports:
        for i in range(benchmark_config.no_physical_sources_per_logical_source):
            for attempt in range(max_retries):
                cmd = f"{TCP_SERVER} -p {port} -n {current_benchmark_config.num_tuples_to_generate} -t {current_benchmark_config.timestamp_increment} -i {current_benchmark_config.ingestion_rate}"
                # Add variable sized data flag to last tcp server
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
def run_benchmark(current_benchmark_config, iteration):
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
        working_dir = create_working_dir()
        copy_and_modify_configs(output_folder, working_dir, current_benchmark_config, tcp_server_ports,
                                iteration)

        # Waiting before starting the single node worker
        time.sleep(WAIT_BETWEEN_COMMANDS)

        # Starting the single node worker
        single_node_process = start_single_node_worker(
            os.path.abspath(os.path.join(output_folder, WORKER_CONFIG_FILE_NAME)))

        # Waiting before submitting the query
        time.sleep(WAIT_BETWEEN_COMMANDS)

        # Submitting the query and waiting couple of seconds before stopping the query
        query_id = submitting_query(os.path.abspath(os.path.join(output_folder, QUERY_CONFIG_FILE_NAME)))

        print(f"Waiting for {MEASURE_INTERVAL}s before stopping the query...")
        time.sleep(MEASURE_INTERVAL)  # Allow query engine stats to be printed
        stop_process = stop_query(query_id)
    finally:
        time.sleep(WAIT_BEFORE_SIGKILL)  # Wait additional time before cleanup
        all_processes = source_processes + [single_node_process] + [stop_process]
        for proc in all_processes:
            terminate_process_if_exists(proc)

        # Move logs and statistics to output folder
        for file_name in os.listdir(os.getcwd()):
            if file_name.startswith("nes-stats-") or file_name.startswith("benchmark-stats-") or file_name.endswith(
                    ".log"):
                source_file = os.path.join(os.getcwd(), file_name)
                shutil.move(source_file, output_folder)

        # Delete working directory
        shutil.rmtree(working_dir)

        return output_folder


def load_csv(file_path):
    if os.path.getsize(file_path) == 0:  # Check if the file is empty
        return pd.DataFrame()  # Return an empty DataFrame
    return pd.read_csv(file_path)


if __name__ == "__main__":
    # Running all benchmarks
    output_folders = []
    ALL_BENCHMARK_CONFIGS = BenchmarkConfig.create_all_benchmark_configs()
    total_iterations = len(ALL_BENCHMARK_CONFIGS)
    iteration_times = []

    for i, benchmark_config in enumerate(ALL_BENCHMARK_CONFIGS, start=1):
        start_time = time.time()

        output_folders.append(run_benchmark(benchmark_config, i))
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
        print(
            f"\033[96mIteration {i}/{total_iterations} completed. ETA: {eta}, Estimated Finish Time: {finish_time.strftime('%Y-%m-%d %H:%M:%S')}\033[0m\n")

    # Compare the results of default slice store and file backed variant
    file1 = f"/tmp/csv_sink_{1}.csv"
    file2 = f"/tmp/csv_sink_{2}.csv"

    df1 = load_csv(file1)
    df2 = load_csv(file2)

    # Check if either DataFrame is empty
    if df1.empty and df2.empty:
        print("Both files are empty.")
    elif df1.empty:
        print("File1 is empty.")
        print(f"File2 has {len(df2)} rows with keys: {list(df2['tcp_source4$id4:UINT64'])}")
    elif df2.empty:
        print("File2 is empty.")
        print(f"File1 has {len(df1)} rows with keys: {list(df1['tcp_source4$id4:UINT64'])}")
    else:
        # Set the key column as the index
        key_column = "tcp_source4$id4:UINT64"
        df1.set_index(key_column, inplace=True)
        df2.set_index(key_column, inplace=True)

        # Align rows based on the key column
        common_keys = df1.index.intersection(df2.index)
        df1_aligned = df1.loc[common_keys]
        df2_aligned = df2.loc[common_keys]

        # Compare rows and find differences
        differences = []
        for key in common_keys:
            if not df1_aligned.loc[key].equals(df2_aligned.loc[key]):
                differences.append((key, df1_aligned.loc[key].to_dict(), df2_aligned.loc[key].to_dict()))

        # Print the differences
        if not differences:
            print("The rows with common keys are identical.")
        else:
            print("Differences found:")
            for diff in differences:
                print(f"Key {diff[0]} differs:")
                print(f"File1: {diff[1]}")
                print(f"File2: {diff[2]}")

        # Identify additional keys in each file
        extra_keys_file1 = df1.index.difference(df2.index)
        extra_keys_file2 = df2.index.difference(df1.index)

        if not extra_keys_file1.empty:
            print(f"File1 has {len(extra_keys_file1)} additional keys: {list(extra_keys_file1)}")
        if not extra_keys_file2.empty:
            print(f"File2 has {len(extra_keys_file2)} additional keys: {list(extra_keys_file2)}")

    # Calling the postprocessing main
    # post_processing = PostProcessing.PostProcessing(output_folders, BENCHMARK_CONFIG_FILE,
    #                                                WORKER_STATISTICS_CSV_PATH,
    #                                                CACHE_STATISTICS_CSV_PATH, PIPELINE_TXT)
    # post_processing.main()

    # all_paths = " tower-en717:/home/nils/remote_server/nebulastream-public/".join(output_folders)
    # copy_command = f"rsync -avz --progress tower-en717:/home/nils/remote_server/nebulastream-public/{all_paths} /home/nils/Downloads/"
    # print(f"Copy command: \n\"{copy_command}\"")

    # output_folders_str = "\",\n\"/home/nils/Downloads/".join(output_folders)
    # print(f"\nFinished running all benchmarks. Output folders: \n\"/home/nils/Downloads/{output_folders_str}\"")
    # print(
    #     f"Created worker statistics CSV file at {WORKER_STATISTICS_CSV_PATH} and cache statistics CSV file at {CACHE_STATISTICS_CSV_PATH}")
    # copy_command = f"rsync -avz --progress tower-en717:{WORKER_STATISTICS_CSV_PATH} tower-en717:{CACHE_STATISTICS_CSV_PATH} /home/nils/Downloads/"
    # print(f"Copy command: \n\"{copy_command}\"")
