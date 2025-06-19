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
import re
import yaml
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing


unit_multipliers = {'': 1, 'k': 10 ** 3, 'M': 10 ** 6, 'G': 10 ** 9, 'T': 10 ** 12}


# This class stores some methods that we need to call after all benchmarks have been run
class PostProcessing:

    def __init__(self, input_folders, measure_interval, startup_time, benchmark_config_file, engine_statistics_file,
                 benchmark_statistics_file, combined_engine_file, combined_benchmark_file, engine_statistics_csv_path,
                 benchmark_statistics_csv_path):
        self.input_folders = input_folders
        self.measure_interval = measure_interval
        self.startup_time = startup_time
        self.benchmark_config_file = benchmark_config_file
        self.engine_statistics_file = engine_statistics_file
        self.benchmark_statistics_file = benchmark_statistics_file
        self.combined_engine_file = combined_engine_file
        self.combined_benchmark_file = combined_benchmark_file
        self.engine_statistics_csv_path = engine_statistics_csv_path
        self.benchmark_statistics_csv_path = benchmark_statistics_csv_path

    def main(self):
        print("Starting post processing...")

        number_of_cores = multiprocessing.cpu_count()
        # with ProcessPoolExecutor(max_workers=number_of_cores) as executor:
        #     futures = {executor.submit(self.convert_engine_statistics_to_csv, f): f for f in self.input_folders}
        #     results = [future.result() for future in tqdm(as_completed(futures), total=len(futures))]
        with ProcessPoolExecutor(max_workers=number_of_cores) as executor:
            futures = {executor.submit(self.convert_benchmark_statistics_to_csv, f): f for f in self.input_folders}
            results = [future.result() for future in tqdm(as_completed(futures), total=len(futures))]

        # Combine the engine and benchmark statistics into two separate csv files and return all folders of failed runs
        # failed_engines = self.combine_engine_statistics()
        failed_benchmarks = self.combine_benchmark_statistics()
        failed_experiments = failed_benchmarks  # + failed_engines
        return failed_experiments

    # Converting query engine statistics to a csv file
    def combine_engine_statistics(self):
        print("Combining all query engine statistics...")
        # Gathering all statistic files across all folders
        statistic_files = [(input_folder_name, os.path.join(input_folder_name, f)) for input_folder_name in
                           self.input_folders for f in os.listdir(input_folder_name) if self.combined_engine_file in f]

        print(f"Found {len(statistic_files)} query engine statistic files in {os.path.dirname(self.input_folders[0])}")
        combined_df = pd.DataFrame()

        for idx, [input_folder, stat_file] in enumerate(statistic_files):
            # print(f"Reading {stat_file} [{idx+1}/{no_statistics_files}]")
            df = pd.read_csv(stat_file)

            # Normalize all timestamps to the minimal start timestamp of any task
            df['start_time'] = pd.to_datetime(df['start_time'], format="%Y-%m-%d %H:%M:%S.%f")
            df['end_time'] = pd.to_datetime(df['end_time'], format="%Y-%m-%d %H:%M:%S.%f")
            min_start_time = df['start_time'].min()
            df['start_time_normalized'] = df['start_time'] - min_start_time
            df['end_time_normalized'] = df['end_time'] - min_start_time

            # Sorting the DataFrame
            df = df.sort_values(by='task_id').reset_index(drop=True)

            # Adding this DataFrame to the global one
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Writing the combined DataFrame to a csv file
        combined_df.to_csv(self.engine_statistics_csv_path, index=False)

        # Return all folders that did not contain a result file
        return [input_folder_name for input_folder_name in self.input_folders if
                self.combined_engine_file not in os.listdir(input_folder_name)]

    # Converting benchmark statistics to a csv file
    def combine_benchmark_statistics(self):
        print("Combining all benchmark statistics...")
        # Gathering all statistic files across all folders
        statistic_files = [(input_folder_name, os.path.join(input_folder_name, f)) for input_folder_name in
                           self.input_folders for f in os.listdir(input_folder_name) if
                           self.combined_benchmark_file in f]

        print(f"Found {len(statistic_files)} benchmark statistic files in {os.path.dirname(self.input_folders[0])}")
        combined_df = pd.DataFrame()

        dtype_map = {
            'max_num_sequence_numbers': 'UInt64',
            'lower_memory_bound': 'UInt64',
            'upper_memory_bound': 'UInt64'
        }

        for idx, [input_folder, stat_file] in enumerate(statistic_files):
            # print(f"Reading {stat_file} [{idx+1}/{no_statistics_files}]")
            df = pd.read_csv(stat_file, dtype=dtype_map)

            # Adding this DataFrame to the global one
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Writing the combined DataFrame to a csv file
        combined_df.to_csv(self.benchmark_statistics_csv_path, index=False)

        # Return all folders that did not contain a result file
        return [input_folder_name for input_folder_name in self.input_folders if
                self.combined_benchmark_file not in os.listdir(input_folder_name)]

    def convert_engine_statistics_to_csv(self, input_folder):
        pattern_engine_statistics_file = rf"^{re.escape(self.engine_statistics_file)}[\w\.\-]+\.stats$"
        pattern_task_details = (r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*?"
                                r"Task (?P<task_id>\d+) for Pipeline (?P<pipeline>\d+).*?"
                                r"(?P<action>Started|Completed)(?:\. Number of Tuples: (?P<num_tuples>\d+))?")

        # Gathering all statistic files. For now, there should only be one
        statistic_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if
                           re.match(pattern_engine_statistics_file, f)]
        combined_df = pd.DataFrame()

        for idx, stat_file in enumerate(statistic_files):
            # print(f"Processing {stat_file} [{idx + 1}/{no_statistics_files}]")
            dir_name = os.path.dirname(stat_file)
            with open(stat_file, 'r') as file:
                log_text = file.read()

            with open(os.path.join(input_folder, self.benchmark_config_file), 'r') as file:
                benchmark_config_yaml = yaml.safe_load(file)

            records = []
            tasks = {}
            for match in re.finditer(pattern_task_details, log_text):
                timestamp = pd.to_datetime(match.group("timestamp"), format="%Y-%m-%d %H:%M:%S.%f")
                task_id = int(match.group("task_id"))
                action = match.group("action")
                num_tuples = int(match.group("num_tuples")) if match.group("num_tuples") else None
                pipeline_id = int(match.group("pipeline")) if match.group("pipeline") else None

                if action == "Started":
                    tasks[task_id] = {"start_time": timestamp, "num_tuples": num_tuples}
                elif action == "Completed" and task_id in tasks:
                    task_info = tasks[task_id]
                    start_time = task_info["start_time"]
                    duration = (timestamp - start_time).total_seconds()
                    throughput = task_info["num_tuples"] / duration if duration > 0 else -1
                    new_record = benchmark_config_yaml.copy()

                    new_record['query'] = new_record['query'].replace(";", "")
                    new_record.update({
                        "dir_name": dir_name,
                        "task_id": task_id,
                        "start_time": start_time,
                        "end_time": timestamp,
                        "duration": duration,  # Seconds
                        "num_tuples": task_info["num_tuples"],
                        "throughput": throughput,  # Tup/s
                        "pipeline_id": pipeline_id
                    })

                    records.append(new_record)

            if len(records) == 0:
                print(f"WARNING: {stat_file} produced no data")
                continue

            # Create DataFrame from records
            df = pd.DataFrame(records)

            # Normalize all timestamps to the minimal start timestamp of any task
            min_start_time = df['start_time'].min()
            df['start_time_normalized'] = df['start_time'] - min_start_time
            df['end_time_normalized'] = df['end_time'] - min_start_time

            # Sort the DataFrame
            df = df.sort_values(by='task_id').reset_index(drop=True)

            # Write the created DataFrame to the csv file
            df.to_csv(stat_file + ".csv", index=False)

            # Adding this dataframe to the global one
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            combined_df.to_csv(os.path.join(input_folder, self.combined_engine_file), index=False)

    def convert_benchmark_statistics_to_csv(self, input_folder):
        interesting_queries = [1]

        pattern_engine_statistics_file = rf"^{re.escape(self.benchmark_statistics_file)}[\w\.\-]+\.stats$"
        pattern_throughput_details = (
            r"Throughput for queryId (?P<query_id>\d+) in window (?P<window_start>\d+)-(?P<window_end>\d+) "
            r"is (?P<throughput_data>[\d\.]+ (?P<data_prefix>[kMGT]?))B/s / (?P<throughput_tuples>[\d\.]+ "
            r"(?P<tuple_prefix>[kMGT]?))Tup/s and memory consumption is (?P<memory>[\d\.]+ (?P<memory_prefix>[kMGT]?))B")

        # Gathering all statistic files. For now, there should only be one
        statistic_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if
                           re.match(pattern_engine_statistics_file, f)]
        combined_df = pd.DataFrame()

        for idx, stat_file in enumerate(statistic_files):
            # print(f"Processing {stat_file} [{idx + 1}/{no_statistics_files}]")
            dir_name = os.path.dirname(stat_file)
            with open(stat_file, 'r') as file:
                log_text = file.read()

            with open(os.path.join(input_folder, self.benchmark_config_file), 'r') as file:
                benchmark_config_yaml = yaml.safe_load(file)

            records = []
            for match in re.finditer(pattern_throughput_details, log_text):
                query_id = int(match.group("query_id"))
                window_start = int(match.group("window_start"))
                # window_end = int(match.group("window_end"))

                # Skip this line if query id is not in interesting_queries
                if interesting_queries is not None and query_id not in interesting_queries:
                    continue

                # Convert throughput_data, throughput_tuples, and memory to base units
                throughput_data = int(
                    float(match.group("throughput_data").split()[0]) * unit_multipliers[match.group("data_prefix")])
                throughput_tuples = int(
                    float(match.group("throughput_tuples").split()[0]) * unit_multipliers[match.group("tuple_prefix")])
                memory = int(float(match.group("memory").split()[0]) * unit_multipliers[match.group("memory_prefix")])

                new_record = benchmark_config_yaml.copy()
                new_record['query'] = new_record['query'].replace(";", "")

                new_record.update({
                    "dir_name": dir_name,
                    "query_id": query_id,
                    "window_start": window_start,
                    "throughput_data": throughput_data,  # B/s
                    "throughput_tuples": throughput_tuples,  # Tup/s
                    "memory": memory  # Bytes
                })

                records.append(new_record)

            if len(records) == 0:
                print(f"WARNING: {stat_file} produced no data")
                continue

            # Create DataFrame from records
            df = pd.DataFrame(records)

            # Shift all timestamps by the minimal window start of any record
            min_start_time = df['window_start'].min()
            df['window_start_normalized'] = df['window_start'] - min_start_time
            # df['window_end_normalized'] = df['window_end'] - min_start_time

            # Keep only relevant data within the measurement interval
            df = df[(df['window_start_normalized'] >= self.startup_time) & (
                        df['window_start_normalized'] <= self.measure_interval)]

            # Sort the DataFrame
            # df = df.sort_values(by=["query_id", "window_start"]).reset_index(drop=True)

            # Write the created DataFrame to the csv file
            df.to_csv(stat_file + ".csv", index=False)

            # Adding this DataFrame to the global one
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            combined_df.to_csv(os.path.join(input_folder, self.combined_benchmark_file), index=False)
