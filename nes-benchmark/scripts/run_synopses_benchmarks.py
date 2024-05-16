#!/usr/bin/env python3

import argparse
import subprocess
import ruamel.yaml
import os
import shutil
import time
import itertools
import pandas as pd
import random

from datetime import datetime, timedelta


# Create the parser
parser = argparse.ArgumentParser(description="Python script to run the Synopses benchmarks.")

# Add the arguments
parser.add_argument("--executable", metavar="executable", type=str, help="the path to the executable")
parser.add_argument("--yaml_file", metavar="yaml_file", type=str, help="the path to the yaml files")
parser.add_argument("--output_folder", metavar="output_folder", type=str, help="the folder that contains all yaml files and the csv file")

# Parse the arguments
args = parser.parse_args()

PRELOG = "numactl -N 0 -m 0"

def get_max_length_of_dict(d):
    return max(len(v) for v in d.values())

def create_all_possible_param_combinations(d):
    all_combinations = list(itertools.product(*d.values()))
    random.shuffle(all_combinations)
    new_dict = {}
    for idx, key in enumerate(d.keys()):
        new_dict[key] = [val[idx] for val in all_combinations]

    return new_dict

def calc_eta(total_experiment_seconds, number_of_experiments, current_experiment):
    hours = int(total_experiment_seconds // 3600)
    minutes = int(total_experiment_seconds % 3600) // 60
    seconds = int(total_experiment_seconds % 60)

    eta_seconds = (number_of_experiments - current_experiment) / (float(current_experiment) / float(total_experiment_seconds))
    eta_datetime = datetime.now() + timedelta(seconds=eta_seconds)

    eta_hours = int(eta_seconds // 3600)
    eta_minutes = int(eta_seconds % 3600) // 60
    eta_seconds = int(eta_seconds % 60)

    return hours, minutes, seconds, eta_hours, eta_minutes, eta_seconds, eta_datetime


def run_benchmark(executable, template_yaml_file, cur_output_folder, cur_params, synopses_params_keys, message=None):

    # As we padded the dictionary, any value has the same length
    number_of_experiments = len(next(iter(cur_params.values())))

    # Run the benchmark for each experiment
    total_experiment_seconds = 0
    for i in range(number_of_experiments):
        current_time_in_seconds = time.time()

        # Create the dictionary with the parameters for the current experiment
        experiment_params = {key : value[i] for key, value in cur_params.items()}

        # Rename the yaml file to be unique, i.e., append the current experiment id to the yaml file
        yaml_file = os.path.join(cur_output_folder, os.path.basename(template_yaml_file).replace(".yaml", "_{}.yaml".format(i)))

        # Create the yaml file for the current experiment
        yaml = ruamel.yaml.YAML()
        yaml.preserve_quotes = True
        yaml.explicit_start = True
        yaml.indent(sequence=4, offset=2)
        with open(template_yaml_file, "r") as template_file:
            template = yaml.load(template_file)
            for key, value in experiment_params.items():
                # If the key is a synopses parameter, replace the value in the query, as we want to change the synopses
                if key in synopses_params_keys:
                    template["query"] = str(template["query"].replace(key, str(value)))
                else:
                    template[key] = value

            # Setting the csv file in the yaml to write to the output folder
            template["outputFile"] = os.path.join(cur_output_folder, template["outputFile"])
            with open(yaml_file, "w+") as file:
                yaml.dump(template, file)

        # Run the experiment with the stdout being redirected to a log file
        log_file = os.path.join(cur_output_folder, "{}_log_{}.txt".format(message, i))
        with open(log_file, "w+") as log:
            subprocess.run(PRELOG.split() + [executable, "--logPath=benchmark.log", "--configPath=" + yaml_file], stdout=log)

        # Printing progress to the console and a simple eta calculation
        current_experiment_duration = time.time() - current_time_in_seconds
        total_experiment_seconds += current_experiment_duration

        hours, minutes, seconds, eta_hours, eta_minutes, eta_seconds, eta_datetime = calc_eta(total_experiment_seconds, number_of_experiments, i + 1)
        print("{} Experiment {}/{} completed at {}: total duration {}h {}m {}s and should be done in approx {}h {}m {}s at {}".format(message, i + 1, number_of_experiments, datetime.now().strftime("%d.%m.%Y %H:%M:%S"), hours, minutes, seconds, eta_hours, eta_minutes, eta_seconds, eta_datetime.strftime("%d.%m.%Y %H:%M:%S")))


############## Params for the synopses ##############
CM_PARAMS = {"width" : [32, 128, 512, 1024, 2048, 8192, 10240],
             "depth" : [1, 2, 5, 10, 20]}
DD_SKETCH_PARAMS = {"error" : [0.01, 0.1, 0.5, 0.75],
                    "pre_allocated_buckets" : [1000, 10000, 100000]}
HYPERLOGLOG_PARAMS = {"width" : [4, 8, 10, 12, 16]}
RESERVOIR_SAMPLE_PARAMS = {"sample_size" : [10, 100, 1000, 10000],
                           "keep_fields" : ["true", "false"]}
FILTER_QUERY_PARAMS = {"selectivity_value" : [100, 1000, 2000, 5000, 7500, 9000]}

############## General Params ##############
GENERAL_PARAMS = {"numberOfWorkerThreads" : [1, 2, 4, 8],
                  "bufferSizeInBytes" : [4096],
                  "numberOfPreAllocatedBuffer" : [10000],
                  "numberOfBuffersToProduce" : [5000000],
                  "numberOfBuffersInGlobalBufferManager": [32768],
                  "numberOfBuffersPerPipeline": [1024],
                  "numberOfBuffersInSourceLocalBufferPool": [1024],
                  "numberOfMeasurementsToCollect" : [10],
                  "startupSleepIntervalInSeconds" : [1]}


############## Yaml File Names ##############
CM_YAML_FILES = ["count_min_with_statistic_sink.yaml", "count_min_without_statistic_sink.yaml"]
DD_SKETCH_YAML_FILES = ["dd_sketch_with_statistic_sink.yaml", "dd_sketch_without_statistic_sink.yaml"]
HYPERLOGLOG_YAML_FILES = ["hyper_log_log_with_statistic_sink.yaml", "hyper_log_log_without_statistic_sink.yaml"]
RESERVOIR_SAMPLE_YAML_FILES = ["reservoir_sample_with_statistic_sink.yaml", "reservoir_sample_without_statistic_sink.yaml"]
FILTER_QUERY_YAML_FILES = ["simple_filter_query.yaml"]
OUTPUT_CSV_FILE_NAME = "all_synopses_benchmarks.csv"


if __name__ == "__main__":
    # 1. Create the output folder by appending the current timestamp in milliseconds to the output folder
    output_folder = os.path.join(args.output_folder, str(int(time.time() * 1000)))
    os.makedirs(output_folder)
    print("Output folder: {}".format(output_folder))

    # 2. We run all benchmarks for the Filter Query
    for yaml_file in FILTER_QUERY_YAML_FILES:
        cur_filter_query_params = create_all_possible_param_combinations({**FILTER_QUERY_PARAMS, **GENERAL_PARAMS})
        run_benchmark(args.executable, os.path.join(args.yaml_file, yaml_file), output_folder, cur_filter_query_params, FILTER_QUERY_PARAMS.keys(), "Filter Query")

    # 3. We run all benchmarks for the Count-Min Sketch
    for yaml_file in CM_YAML_FILES:
        cur_cm_params = create_all_possible_param_combinations({**CM_PARAMS, **GENERAL_PARAMS})
        run_benchmark(args.executable, os.path.join(args.yaml_file, yaml_file), output_folder, cur_cm_params, CM_PARAMS.keys(), "Count-Min Sketch")

    # 4. We run all benchmarks for the DD Sketch
    for yaml_file in DD_SKETCH_YAML_FILES:
        cur_dd_sketch_params = create_all_possible_param_combinations({**DD_SKETCH_PARAMS, **GENERAL_PARAMS})
        run_benchmark(args.executable, os.path.join(args.yaml_file, yaml_file), output_folder, cur_dd_sketch_params, DD_SKETCH_PARAMS.keys(), "DD Sketch")

    # 5. We run all benchmarks for the HyperLogLog
    for yaml_file in HYPERLOGLOG_YAML_FILES:
        cur_hyperloglog_params = create_all_possible_param_combinations({**HYPERLOGLOG_PARAMS, **GENERAL_PARAMS})
        run_benchmark(args.executable, os.path.join(args.yaml_file, yaml_file), output_folder, cur_hyperloglog_params, HYPERLOGLOG_PARAMS.keys(), "HyperLogLog")

    # # 6. We run all benchmarks for the Equi-Width Histogram
    # for yaml_file in EQUI_WIDTH_HISTOGRAM_YAML_FILES:
    #     cur_equi_width_histogram_params = create_all_possible_param_combinations({**EQUI_WIDTH_HISTOGRAM_PARAMS, **GENERAL_PARAMS})
    #     run_benchmark(args.executable, os.path.join(args.yaml_file, yaml_file), output_folder, cur_equi_width_histogram_params, EQUI_WIDTH_HISTOGRAM_PARAMS.keys(), "Equi-Width Histogram")

    # 7. We run all benchmarks for the Reservoir Sample
    for yaml_file in RESERVOIR_SAMPLE_YAML_FILES:
        cur_reservoir_sample_params = create_all_possible_param_combinations({**RESERVOIR_SAMPLE_PARAMS, **GENERAL_PARAMS})
        run_benchmark(args.executable, os.path.join(args.yaml_file, yaml_file), output_folder, cur_reservoir_sample_params, RESERVOIR_SAMPLE_PARAMS.keys(), "Reservoir Sample")

    # 8. Create one large csv file with a new column ("synopsis") that contains the name of the synopsis
    csv_files = [f for f in os.listdir(output_folder) if f.endswith(".csv")]
    all_synopses_df = pd.DataFrame()
    for csv_file in csv_files:
        df = pd.read_csv(os.path.join(output_folder, csv_file))
        df["synopsis"] = csv_file.replace(".csv", "")
        all_synopses_df = pd.concat([all_synopses_df, df])
    all_synopses_df.to_csv(os.path.join(output_folder, OUTPUT_CSV_FILE_NAME), index=False)

    # 9. Post-process all csv files by removing all duplicate lines
    csv_files = [f for f in os.listdir(output_folder) if f.endswith(".csv")]
    for csv_file in csv_files:
        with open(os.path.join(output_folder, csv_file), "r") as input_file:
            lines = input_file.readlines()
            unique_lines = list(set(lines))
            with open(os.path.join(output_folder, csv_file), "w") as output_file:
                for line in lines:
                    if line in unique_lines:
                        output_file.writelines(line)
                        unique_lines.remove(line)

    print("All benchmarks are done. The results are stored in the output folder: {}".format(output_folder))