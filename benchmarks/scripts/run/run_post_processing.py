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
import PostProcessing


# Compilation for misc.
DATETIME = "2025-06-02_20-08-46"
RESULTS_DIR = f"benchmarks/data/{DATETIME}"
WORKING_DIR = f".cache/benchmarks/{DATETIME}"
COMBINED_ENGINE_STATISTICS_FILE = "combined_engine_statistics.csv"
COMBINED_BENCHMARK_STATISTICS_FILE = "combined_benchmark_statistics.csv"
BENCHMARK_STATS_FILE = "BenchmarkStats_"
ENGINE_STATS_FILE = "EngineStats_"
BENCHMARK_CONFIG_FILE = "benchmark_config.yaml"

# Configuration for benchmark run
MEASURE_INTERVAL = 8
WAIT_BETWEEN_COMMANDS = 2


def create_results_dir():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    # print(f"Created results dir {folder_name}...")
    return [os.path.join(RESULTS_DIR, COMBINED_ENGINE_STATISTICS_FILE),
            os.path.join(RESULTS_DIR, COMBINED_BENCHMARK_STATISTICS_FILE)]


def get_subdirectories_with_paths(directory):
    return [os.path.join(directory, d) for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]


if __name__ == "__main__":
    output_folders = get_subdirectories_with_paths(WORKING_DIR)
    print(f"Found {len(output_folders)} directories")

    # Calling the postprocessing main
    measurement_time = MEASURE_INTERVAL * 1000
    startup_time = WAIT_BETWEEN_COMMANDS * 1000
    engine_stats_csv_path, benchmark_stats_csv_path = create_results_dir()
    post_processing = PostProcessing.PostProcessing(output_folders, measurement_time, startup_time,
                                                    BENCHMARK_CONFIG_FILE, ENGINE_STATS_FILE, BENCHMARK_STATS_FILE,
                                                    COMBINED_ENGINE_STATISTICS_FILE, COMBINED_BENCHMARK_STATISTICS_FILE,
                                                    engine_stats_csv_path, benchmark_stats_csv_path)
    failed_run_folders = post_processing.main()

    if not failed_run_folders:
        print("Post processing completed successfully.")
    else:
        print(f"{len(failed_run_folders)} runs failed:\n{failed_run_folders}")
