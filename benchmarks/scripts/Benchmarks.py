#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 09:04:05 2025

@author: ntantow
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the CSV file
df = pd.read_csv("../data/amd/2025-06-05_10-15-06/combined_benchmark_statistics.csv")

# Define configuration parameters and measurement columns
config_params = [
    'buffer_size_in_bytes', 'buffers_in_global_buffer_manager',
    'buffers_in_source_local_buffer_pool', 'buffers_per_worker',
    'execution_mode', 'file_descriptor_buffer_size', 'file_layout',
    'file_operation_time_delta', 'ingestion_rate',
    'max_num_sequence_numbers', 'min_read_state_size',
    'min_write_state_size', 'num_watermark_gaps_allowed',
    'number_of_worker_threads', 'page_size', 'query',
    'slice_store_type', 'task_queue_size', 'timestamp_increment',
    'watermark_predictor_type'
]

measurement_columns = ['throughput_data', 'memory', 'window_start_normalized']

# Group by configuration parameters to identify unique experiments
grouped = df.groupby(config_params)

# Function to normalize and aggregate data
def normalize_and_aggregate(group):
    group['window_start_normalized'] = (group['window_start_normalized'] - group['window_start_normalized'].min()) / (
            group['window_start_normalized'].max() - group['window_start_normalized'].min())
    return group.mean()

# Apply normalization and aggregation
aggregated_data = grouped.apply(normalize_and_aggregate).reset_index()

# Visualization: Baseline vs. Prototype Performance
def plot_baseline_vs_prototype(data, config):
    config_data = data
    for param, value in config.items():
        config_data = config_data[config_data[param] == value]

    if config_data.empty or len(config_data['slice_store_type'].unique()) < 2:
        print(f"No data available for both slice store types for configuration: {config}")
        return

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=config_data[config_data['slice_store_type'] == 'Default'], x='window_start_normalized',
                 y='throughput_data', label='Default Throughput')
    sns.lineplot(data=config_data[config_data['slice_store_type'] == 'File-Backed'], x='window_start_normalized',
                 y='throughput_data', label='File-Backed Throughput')
    plt.title(f'Throughput Comparison for Configuration')
    plt.xlabel('Normalized Time')
    plt.ylabel('Throughput')
    plt.legend()
    plt.show()

# Example usage for a specific configuration
sample_config = aggregated_data[config_params].iloc[0].to_dict()
plot_baseline_vs_prototype(aggregated_data, sample_config)

# Visualization: Parameter Effects for Shared Parameters
shared_parameters = [
    'buffer_size_in_bytes', 'buffers_in_global_buffer_manager',
    'buffers_in_source_local_buffer_pool', 'buffers_per_worker',
    'execution_mode', 'ingestion_rate', 'number_of_worker_threads',
    'page_size', 'query', 'task_queue_size', 'timestamp_increment'
]

for param in shared_parameters:
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=aggregated_data, x=param, y='throughput_data', hue='slice_store_type', ci=None)
    plt.title(f'Effect of {param} on Throughput')
    plt.xlabel(param)
    plt.ylabel('Average Throughput')
    plt.legend(title='Slice Store Type')
    plt.show()

# Visualization: Parameter Effects for File-Backed Only Parameters
file_backed_parameters = [
    'file_descriptor_buffer_size', 'file_layout', 'file_operation_time_delta',
    'max_num_sequence_numbers', 'min_read_state_size', 'min_write_state_size',
    'num_watermark_gaps_allowed', 'watermark_predictor_type'
]

file_backed_data = aggregated_data[aggregated_data['slice_store_type'] == 'File-Backed']

for param in file_backed_parameters:
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=file_backed_data, x=param, y='throughput_data', ci=None, color='blue')
    plt.title(f'Effect of {param} on Throughput (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel('Average Throughput')
    plt.show()
