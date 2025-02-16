#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor

from numpy.lib.format import BUFFER_SIZE

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

# This class stores some methods that we need to call after all benchmarks have been run
class PostProcessing:

    def __init__(self, input_folders, benchmark_config_file, combined_csv_file_name):
        self.input_folders = input_folders
        self.benchmark_config_file = benchmark_config_file
        self.combined_csv_file_name = combined_csv_file_name

    def main(self):
        print("Starting post processing")

        number_of_cores = multiprocessing.cpu_count()
        with ProcessPoolExecutor(max_workers=number_of_cores) as executor:
            futures = {executor.submit(self.convert_statistics_to_csv, f): f for f in self.input_folders}
            results = [future.result() for future in tqdm(as_completed(futures), total=len(futures))]

        print("Finished post processing")

    def convert_statistics_to_csv(self, input_folder):
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
            print(f"Processing {stat_file} [{idx + 1}/{no_statistics_files}]")
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

            # Write the created dataframe to the csv file
            df.to_csv(stat_file + ".csv", index=False)

            # Adding this dataframe to the global one
            cnt_rows += len(df)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            combined_df.to_csv(os.path.join(input_folder, self.combined_csv_file_name), index=False)
