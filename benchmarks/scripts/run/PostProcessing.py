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


# Converting a cache hits and misses file to two dictionaries
def parse_lines_to_dataframe(file):
    with open(file, 'r') as input_file:
        lines = input_file.readlines()

    # Initialize dictionaries to hold the hits and misses data
    hits_data = {}
    misses_data = {}

    # Regular expression to extract information
    pattern = re.compile(r'Hits: (\d+) Misses: (\d+) for worker thread (\d+)(?: and join build side (\w+))?')

    for line in lines:
        match = pattern.search(line)
        if match:
            hits, misses, thread_id, build_side = match.groups()
            # Determine the column name
            if build_side:
                column_name = f'worker_{thread_id}_{build_side}'
            else:
                column_name = f'worker_{thread_id}'

            # Store the hits and misses in the dictionaries with prefixes
            hits_column_name = f'hits_{column_name}'
            misses_column_name = f'misses_{column_name}'

            if hits_column_name not in hits_data:
                hits_data[hits_column_name] = []
                misses_data[misses_column_name] = []

            hits_data[hits_column_name] = (int(hits))
            misses_data[misses_column_name] = (int(misses))

    # Adding a total hits and misses column
    hits_data["hits_total"] = sum(hit for hit in hits_data.values())
    misses_data["misses_total"] = sum(miss for miss in misses_data.values())

    return hits_data, misses_data


def find_pipeline_number(log_text):
    lines = log_text.split('\n')
    for i, line in enumerate(lines):
        if "Pipeline:" in line:
            pipeline_number = int(line.split(":")[1].strip())
            # Check the subsequent lines for "Build" until the next "Pipeline"
            for j in range(i + 1, len(lines)):
                if "Pipeline:" in lines[j]:
                    break
                elif "Build" in lines[j]:
                    return [pipeline_number]
    return None


# This class stores some methods that we need to call after all benchmarks have been run
class PostProcessing:

    def __init__(self, input_folders, benchmark_config_file, combined_csv_file_name, worker_statistics_csv_path, cache_statistics_csv_path, cache_hits_misses_name, pipeline_txt):
        self.input_folders = input_folders
        self.benchmark_config_file = benchmark_config_file
        self.worker_statistics_csv_path = worker_statistics_csv_path
        self.cache_statistics_csv_path = cache_statistics_csv_path
        self.cache_hits_misses_name = cache_hits_misses_name
        self.combined_csv_file_name = combined_csv_file_name
        self.pipeline_txt = pipeline_txt


    def main(self):
        print("Starting post processing")

        number_of_cores = multiprocessing.cpu_count()
        with ProcessPoolExecutor(max_workers=number_of_cores) as executor:
            futures = {executor.submit(self.convert_statistics_to_csv, f): f for f in self.input_folders}
            results = [future.result() for future in tqdm(as_completed(futures), total=len(futures))]

        # Now, we can combine the worker and the cache statistics into two separate csv files
        self.combine_worker_statistics()
        self.combine_cache_statistics()

        print("Finished post processing")


    # Gathering all cache statistic files across all folders
    def combine_cache_statistics(self):
        cache_statistic_files = [(input_folder_name, os.path.join(input_folder_name, f)) for input_folder_name in self.input_folders for f in os.listdir(input_folder_name) if self.cache_hits_misses_name in f]
        print(f"Found {len(cache_statistic_files)} cache statistic files in {self.input_folders}")
        cache_stats_combined_df = pd.DataFrame()
        for idx, [input_folder, cache_stat_file] in enumerate(cache_statistic_files):
            # Reading the benchmark configs and the hits and misses
            with open(os.path.join(input_folder, self.benchmark_config_file), 'r') as file:
                benchmark_config_yaml = yaml.safe_load(file)

            (hits_dict, misses_dict) = parse_lines_to_dataframe(cache_stat_file)

            # Combine the dictionaries
            combined_dict = {**hits_dict, **misses_dict, **benchmark_config_yaml.copy()}
            new_row_df = pd.DataFrame([combined_dict])
            cache_stats_combined_df = pd.concat([cache_stats_combined_df, new_row_df], ignore_index=True)

        cache_stats_combined_df = cache_stats_combined_df.fillna(0, downcast='infer')
        cache_stats_combined_df["slice_cache"] = cache_stats_combined_df["slice_cache_type"].astype(str) + "_" + cache_stats_combined_df["numberOfEntriesSliceCache"].astype(str)

        # Writing the combined dataframe to a csv file
        cache_stats_combined_df.to_csv(self.cache_statistics_csv_path, index=False)
        print(f"Done with combining all cache statistics to {self.cache_statistics_csv_path}!")

    # Converting query engine statistics to a csv file
    def combine_worker_statistics(self):
        # Gathering all statistic files across all folders
        statistic_files = [(input_folder_name, os.path.join(input_folder_name, f)) for input_folder_name in self.input_folders for f in os.listdir(input_folder_name) if self.combined_csv_file_name in f]
        print(f"Found {len(statistic_files)} worker statistic files in {self.input_folders}")
        combined_df = pd.DataFrame()
        no_statistics_files = len(statistic_files)
        cnt_rows = 0
        for idx, [input_folder, stat_file] in enumerate(statistic_files):
            #print(f"Reading {stat_file} [{idx+1}/{no_statistics_files}]")
            df = pd.read_csv(stat_file)

            # Normalize all timestamps to the minimal start timestamp of any task
            df['start_time'] = pd.to_datetime(df['start_time'], format="%Y-%m-%d %H:%M:%S.%f")
            df['end_time'] = pd.to_datetime(df['end_time'], format="%Y-%m-%d %H:%M:%S.%f")
            min_start_time = df['start_time'].min()
            df['start_time_normalized'] = df['start_time'] - min_start_time
            df['end_time_normalized'] = df['end_time'] - min_start_time

            # Sorting the dataframe
            df = df.sort_values(by='task_id').reset_index(drop=True)

            # Adding this dataframe to the global one
            cnt_rows += len(df)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        combined_df["slice_cache"] = combined_df["slice_cache_type"].astype(str) + "_" + combined_df["numberOfEntriesSliceCache"].astype(str)
        combined_df["degree_of_disorder"] = combined_df["degree_of_disorder"].astype(str) + "_" + combined_df["shuffle_strategy"].astype(str)

        # Writing the combined dataframe to a csv file
        combined_df.to_csv(self.worker_statistics_csv_path, index=False)
        print(f"Done with combining all query engine statistics to {self.worker_statistics_csv_path}!")

    def convert_statistics_to_csv(self, input_folder):
        pipeline_txt_path = os.path.join(input_folder, self.pipeline_txt)
        with open(pipeline_txt_path, 'r') as file:
            interesting_pipelines = find_pipeline_number(file.read())

        pattern_worker_file = r"^worker_\d+\.txt$"
        pattern_task_details = (r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*?"
                                r"Task (?P<task_id>\d+) for Pipeline (?P<pipeline>\d+).*?"
                                r"(?P<action>Started|Completed)(?:\. Number of Tuples: (?P<num_tuples>\d+))?")

        # Gathering all statistic files across all folders
        statistic_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if re.match(pattern_worker_file, f)]
        combined_df = pd.DataFrame()
        no_statistics_files = len(statistic_files)
        cnt_rows = 0
        for idx, stat_file in enumerate(statistic_files):
            #print(f"Processing {stat_file} [{idx + 1}/{no_statistics_files}]")
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
                        "task_id": task_id,
                        "start_time": start_time,
                        "end_time": timestamp,
                        "duration": duration,  # Seconds
                        "num_tuples": task_info["num_tuples"],
                        "throughput": throughput,  # tup/s
                        "pipeline_id": pipeline_id
                    })

                    records.append(new_record)

            # Create DataFrame and
            df = pd.DataFrame(records)

            # Normalize all timestamps to the minimal start timestamp of any task
            min_start_time = df['start_time'].min()
            df['start_time_normalized'] = df['start_time'] - min_start_time
            df['end_time_normalized'] = df['end_time'] - min_start_time

            # Sorting the dataframe
            df = df.sort_values(by='task_id').reset_index(drop=True)

            # Remove everything that has a pipeline id not in interesting_pipelines
            if interesting_pipelines is not None:
                df = df[df['pipeline_id'].isin(interesting_pipelines)]

            # Write the created dataframe to the csv file
            df.to_csv(stat_file + ".csv", index=False)

            # Adding this dataframe to the global one
            cnt_rows += len(df)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            combined_df.to_csv(os.path.join(input_folder, self.combined_csv_file_name), index=False)
