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
import numpy as np
import pandas as pd
import BenchmarkConfig
import PostProcessing


# Configuration for compilation
BUILD_PROJECT = False
BUILD_DIR = "cmake-build-relnologging"
NES_DIR = "nebulastream-public_1"
SOURCE_DIR = os.path.join("/home/ntantow/Documents/NebulaStream", NES_DIR)
CMAKE_TOOLCHAIN_FILE = "/home/ntantow/Documents/NebulaStream/vcpkg/scripts/buildsystems/vcpkg.cmake"
LOG_LEVEL = "LEVEL_NONE"
CMAKE_BUILD_TYPE = "Release"
USE_LIBCXX_IF_AVAILABLE = "False"  # False for using libstdcxx, True for libcxx
ENABLE_LARGE_TESTS = 1
SYSTEST_PATH = os.path.join("$(pwd)", BUILD_DIR, "nes-systests/systest/systest")
TEST_DATA = os.path.join("$(pwd)", BUILD_DIR, "nes-systests/testdata")

# Configuration for benchmark run
NUM_RUNS_PER_CONFIG = 3
NUM_RETRIES_PER_RUN = 3
MEASURE_INTERVAL = 8
WAIT_BETWEEN_COMMANDS = 2
WAIT_BEFORE_SIGKILL = 5

# Compilation for misc.
DELETE_ENGINE_STATS = True
SERVER_NAME = "amd"
DESTINATION_PATH = os.path.join("/home/ntantow/Downloads/ba-benchmark/", SERVER_NAME)
DATETIME_NOW = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
RESULTS_DIR = f"benchmarks/data/{DATETIME_NOW}"
WORKING_DIR = f".cache/benchmarks/{DATETIME_NOW}"
ERROR_FILE_PATH = os.path.join(RESULTS_DIR, "failed_systests.txt")
COMBINED_ENGINE_STATISTICS_FILE = "combined_engine_statistics.csv"
COMBINED_BENCHMARK_STATISTICS_FILE = "combined_benchmark_statistics.csv"
COMBINED_SLICE_ACCESSES_FILE = "combined_slice_accesses.csv"
BENCHMARK_STATS_FILE = "BenchmarkStats_"
ENGINE_STATS_FILE = "EngineStats_"
SLICE_ACCESSES_FILE = "SliceAccesses_"
BENCHMARK_CONFIG_FILE = "benchmark_config.yaml"
TEST_NAME = "Nexmark.test:05"
TEST_FILE_PATH = os.path.join("$(pwd)", "nes-systests/benchmark", TEST_NAME)


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
                 f"-DENABLE_LARGE_TESTS={ENABLE_LARGE_TESTS}",
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
    folder_name = os.path.join(WORKING_DIR, f"SpillingSystests_{timestamp}")
    os.makedirs(folder_name, exist_ok=True)
    print(f"Created folder {folder_name}...")
    return folder_name


def start_systest(output_folder, working_dir, current_benchmark_config):
    # Write the current benchmark config to the output folder in a way that it can be easily read
    # We use a dictionary representation of the configuration
    with open(os.path.join(output_folder, BENCHMARK_CONFIG_FILE), 'w') as output_file:
        yaml.dump(current_benchmark_config.to_dict(), output_file)

    cmd = f"{SYSTEST_PATH} -t {TEST_FILE_PATH} -b --workingDir={working_dir} --timeout={MEASURE_INTERVAL} --data {TEST_DATA} --groups large -- " \
          f"--worker.queryEngine.numberOfWorkerThreads={current_benchmark_config.number_of_worker_threads} " \
          f"--worker.queryEngine.taskQueueSize={current_benchmark_config.task_queue_size} " \
          f"--worker.bufferSizeInBytes={current_benchmark_config.buffer_size_in_bytes} " \
          f"--worker.numberOfBuffersInGlobalBufferManager={current_benchmark_config.buffers_in_global_buffer_manager} " \
          f"--worker.numberOfBuffersInSourceLocalPools={current_benchmark_config.buffers_in_source_local_buffer_pool} " \
          f"--worker.throughputListenerTimeInterval={current_benchmark_config.throughput_listener_time_interval} " \
          f"--worker.defaultQueryExecution.executionMode={current_benchmark_config.execution_mode} " \
          f"--worker.defaultQueryExecution.pageSize={current_benchmark_config.page_size} " \
          f"--worker.defaultQueryExecution.sliceStoreType={current_benchmark_config.slice_store_type} " \
          f"--worker.defaultQueryExecution.lowerMemoryBound={current_benchmark_config.lower_memory_bound} " \
          f"--worker.defaultQueryExecution.upperMemoryBound={current_benchmark_config.upper_memory_bound} " \
          f"--worker.defaultQueryExecution.maxNumWatermarkGaps={current_benchmark_config.max_num_watermark_gaps} " \
          f"--worker.defaultQueryExecution.maxNumSequenceNumbers={current_benchmark_config.max_num_sequence_numbers} " \
          f"--worker.defaultQueryExecution.minReadStateSize={current_benchmark_config.min_read_state_size} " \
          f"--worker.defaultQueryExecution.minWriteStateSize={current_benchmark_config.min_write_state_size} " \
          f"--worker.defaultQueryExecution.predictionTimeDelta={current_benchmark_config.prediction_time_delta} " \
          f"--worker.defaultQueryExecution.withCleanup={current_benchmark_config.with_cleanup} " \
          f"--worker.defaultQueryExecution.withPrediction={current_benchmark_config.with_prediction} " \
          f"--worker.defaultQueryExecution.fileLayout={current_benchmark_config.file_layout} " \
          f"--worker.defaultQueryExecution.watermarkPredictorType={current_benchmark_config.watermark_predictor_type} " \
          f"--worker.defaultQueryExecution.maxNumFileDescriptors={current_benchmark_config.max_num_file_descriptors} " \
          f"--worker.defaultQueryExecution.fileDescriptorBufferSize={current_benchmark_config.file_descriptor_buffer_size} " \
          f"--worker.defaultQueryExecution.numberOfBuffersPerWorker={current_benchmark_config.num_buffers_per_worker}"
    # print(f"Starting the systest with {cmd}")
    process = subprocess.Popen(cmd, shell=True, cwd=SOURCE_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    pid = process.pid
    print(f"Started systest with pid {pid}")
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


# Benchmark loop
def run_benchmark(current_benchmark_config):
    # Defining here two empty lists so that we can use it in the finally
    output_folder = ""
    working_dir = ""
    systest_process = []
    try:
        # Creating a new output folder and working directory
        output_folder = create_output_folder()
        working_dir = create_working_dir(output_folder)

        # Starting the systest
        systest_process = start_systest(output_folder, "$(pwd)/" + working_dir, current_benchmark_config)

        # Waiting for the systest to start
        time.sleep(WAIT_BETWEEN_COMMANDS)

        print(f"Waiting for max {MEASURE_INTERVAL}s before stopping the systest...")
        try:
            systest_process.wait(timeout=MEASURE_INTERVAL)  # Allow query engine stats to be printed
        finally:
            print("Stopping the systest...")

            _, stderr = systest_process.communicate(timeout=WAIT_BEFORE_SIGKILL)
            if stderr != '':
                with open(ERROR_FILE_PATH, "a") as f:
                    f.write(output_folder + ": " + stderr + "\n")
    finally:
        time.sleep(WAIT_BEFORE_SIGKILL)  # Wait additional time before cleanup
        all_processes = [systest_process]
        for proc in all_processes:
            terminate_process_if_exists(proc)

        # Move logs and statistics to output folder
        for file_name in os.listdir(os.getcwd()):
            if file_name.startswith(ENGINE_STATS_FILE) or file_name.startswith(
                    ENGINE_STATS_FILE) or file_name.startswith(SLICE_ACCESSES_FILE) or file_name.endswith(".log"):
                source_file = os.path.join(os.getcwd(), file_name)
                shutil.move(source_file, output_folder)

        # Delete working directory
        print("Deleting working directory...")
        shutil.rmtree(working_dir)

        if DELETE_ENGINE_STATS:
            print("Deleting engine statistics...")
            for file_name in os.listdir(output_folder):
                if file_name.startswith(ENGINE_STATS_FILE):
                    engine_stats_file_path = os.path.join(output_folder, file_name)
                    os.remove(engine_stats_file_path)

        return output_folder


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


def main():
    if BUILD_PROJECT:
        # Removing the build folder and compiling the project
        clear_build_dir()
        compile_project()

    # Remove tmp directory as benchmarks will fail if another user created this
    if subprocess.Popen("rm -rf /tmp/dump", shell=True).returncode is not None:
        print("Could not remove /tmp/dump. Restart after executing\nsudo rm -rf /tmp/dump")
        return

    # We use pwd to create file paths which will only work from nes root directory
    process = subprocess.Popen(f"cat {TEST_FILE_PATH.split(":")[0]}", shell=True, stdout=subprocess.DEVNULL,stderr=subprocess.PIPE, text=True)
    _, stderr = process.communicate()
    if stderr != '':
        print("Execute from nebulastream root directory")
        return

    # If executable was built with docker we need to create a symlink for testdata
    subprocess.Popen(f"ln -s $(pwd) {os.path.join("/tmp", NES_DIR)}", shell=True)

    print("################################################################")
    print("Running systest main")
    print("################################################################\n")
    ALL_SYSTEST_CONFIGS = BenchmarkConfig.create_systest_configs()

    engine_stats_csv_path, benchmark_stats_csv_path = create_results_dir()
    with open(ERROR_FILE_PATH, "w") as f:
        f.write("Errors in Systest: \n")

    for attempt in range(NUM_RETRIES_PER_RUN):
        num_runs_per_config = NUM_RUNS_PER_CONFIG if attempt == 0 else 1

        # Running all benchmarks
        output_folders = []
        iteration_times = []
        num_configs = len(ALL_SYSTEST_CONFIGS)
        total_iterations = num_configs * num_runs_per_config
        print(f"Attempt {attempt + 1} of {NUM_RETRIES_PER_RUN} of running each of the {num_configs} experiments "
              f"{num_runs_per_config} times, totaling {total_iterations} runs...\n")

        for j in range(num_runs_per_config):
            for i, systest_config in enumerate(ALL_SYSTEST_CONFIGS, start=(j * num_configs + 1)):
                start_time = time.time()

                output_folders.append(run_benchmark(systest_config))
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
                print(f"\033[96mRun {i}/{total_iterations} in attempt {attempt + 1} completed. ETA: {eta}, "
                      f"Estimated Finish Time: {finish_time.strftime('%Y-%m-%d %H:%M:%S')}\033[0m\n")

        # Calling the postprocessing main
        measurement_time = MEASURE_INTERVAL * 1000
        startup_time = 0
        post_processing = PostProcessing.PostProcessing(output_folders,
                                                        measurement_time,
                                                        startup_time,
                                                        BENCHMARK_CONFIG_FILE,
                                                        ENGINE_STATS_FILE,
                                                        BENCHMARK_STATS_FILE,
                                                        SLICE_ACCESSES_FILE,
                                                        COMBINED_ENGINE_STATISTICS_FILE,
                                                        COMBINED_BENCHMARK_STATISTICS_FILE,
                                                        COMBINED_SLICE_ACCESSES_FILE,
                                                        engine_stats_csv_path,
                                                        benchmark_stats_csv_path,
                                                        SERVER_NAME,
                                                        TEST_NAME)
        failed_run_folders = post_processing.main()

        # Re-running all runs that failed
        if not failed_run_folders:
            print(f"\nAll benchmarks completed successfully in {attempt + 1} attempt(s).")
            break
        else:
            print(f"\n{len(failed_run_folders)} runs failed in attempt {attempt + 1}. Retrying...\n")
            ALL_SYSTEST_CONFIGS = [
                BenchmarkConfig.BenchmarkConfig(
                    **yaml.safe_load(open(os.path.join(failed_run, BENCHMARK_CONFIG_FILE), 'r')))
                for failed_run in failed_run_folders
            ]
    else:
        with open(ERROR_FILE_PATH, "a") as f:
            f.write("\nNo measurements available for the following runs:\n")
            f.write(", ".join(failed_run_folders) + "\n")
        print(f"Maximum retries reached. Some runs still failed:\n{failed_run_folders}")

    results_path = os.path.join(SOURCE_DIR, RESULTS_DIR)
    copy_command = f"scp -r {SERVER_NAME}:{results_path} {DESTINATION_PATH}"
    print(f"\n\033[96mSize of results is {format_data_size(get_directory_size(results_path))}. "
          f"Copy with:\n{copy_command}\033[0m")


if __name__ == "__main__":
    main()
