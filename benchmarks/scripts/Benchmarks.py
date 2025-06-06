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
def plot_slice_store_performance(data, slice_store_type):
    sample_config_data = data[data['slice_store_type'] == slice_store_type].iloc[0:1]

    if sample_config_data.empty:
        print(f"No data available for slice store type: {slice_store_type}")
        return

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=sample_config_data, x='window_start_normalized', y='throughput_data',
                 label=f'{slice_store_type} Throughput')
    plt.title(f'Throughput for {slice_store_type} Slice Store')
    plt.xlabel('Normalized Time')
    plt.ylabel('Throughput')
    plt.legend()
    plt.show()

# Plot performance for each slice store type
plot_slice_store_performance(aggregated_data, 'Default')
plot_slice_store_performance(aggregated_data, 'File-Backed')

# Visualization: Parameter Effects for Shared Parameters
shared_parameters = [
    'buffer_size_in_bytes', 'buffers_in_global_buffer_manager',
    'buffers_in_source_local_buffer_pool', 'buffers_per_worker',
    'execution_mode', 'ingestion_rate', 'number_of_worker_threads',
    'page_size', 'query', 'task_queue_size', 'timestamp_increment'
]

for param in shared_parameters:
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=aggregated_data, x=param, y='throughput_data', hue='slice_store_type', errorbar=None)
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
    if param in file_backed_data.columns:
        plt.figure(figsize=(14, 6))
        sns.lineplot(data=file_backed_data, x=param, y='throughput_data', errorbar=None, color='blue')
        plt.title(f'Effect of {param} on Throughput (File-Backed Only)')
        plt.xlabel(param)
        plt.ylabel('Average Throughput')
        plt.show()
    else:
        print(f"Parameter {param} not found in the dataset.")
