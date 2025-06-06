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
shared_config_params = [
    'buffer_size_in_bytes', 'buffers_in_global_buffer_manager',
    'buffers_in_source_local_buffer_pool', 'buffers_per_worker',
    'execution_mode', 'ingestion_rate', 'number_of_worker_threads',
    'page_size', 'query', 'task_queue_size', 'timestamp_increment'
]

file_backed_config_params = [
    'file_descriptor_buffer_size', 'file_layout', 'file_operation_time_delta',
    'max_num_sequence_numbers', 'min_read_state_size', 'min_write_state_size',
    'num_watermark_gaps_allowed', 'watermark_predictor_type'
]

measurement_columns = ['throughput_data', 'memory', 'window_start_normalized']

# Normalize and aggregate data
def normalize_and_aggregate(group):
    group['window_start_normalized'] = (group['window_start_normalized'] - group['window_start_normalized'].min()) / (
            group['window_start_normalized'].max() - group['window_start_normalized'].min())
    return group.mean()

# Apply normalization and aggregation
df['window_start_normalized'] = df.groupby(shared_config_params)['window_start_normalized'].transform(
    lambda x: (x - x.min()) / (x.max() - x.min()))
aggregated_data = df.groupby(shared_config_params + ['slice_store_type', 'window_start_normalized']).mean().reset_index()

# Visual Comparison Plot
def plot_comparison(data):
    plt.figure(figsize=(14, 6))

    # Find a common configuration for both slice store types
    common_config = data[shared_config_params].drop_duplicates().iloc[0].to_dict()

    # Filter data for the common configuration
    default_data = data[(data['slice_store_type'] == 'Default') & (data[shared_config_params] == pd.Series(common_config)).all(axis=1)]
    file_backed_data = data[(data['slice_store_type'] == 'File-Backed') & (data[shared_config_params] == pd.Series(common_config)).all(axis=1)]

    if not default_data.empty and not file_backed_data.empty:
        sns.lineplot(data=default_data, x='window_start_normalized', y='throughput_data', label='Default Throughput')
        sns.lineplot(data=file_backed_data, x='window_start_normalized', y='throughput_data', label='File-Backed Throughput')
        plt.title('Throughput Comparison: Default vs. File-Backed')
        plt.xlabel('Normalized Time')
        plt.ylabel('Throughput')
        plt.legend()
        plt.show()
    else:
        print("No matching configurations found for both slice store types.")

plot_comparison(aggregated_data)

# Shared Parameter Plots
for param in shared_config_params:
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=aggregated_data, x=param, y='throughput_data', hue='slice_store_type', errorbar=None)
    plt.title(f'Effect of {param} on Throughput')
    plt.xlabel(param)
    plt.ylabel('Average Throughput')
    plt.legend(title='Slice Store Type')
    plt.show()

# File-Backed Only Parameter Plots
file_backed_data = aggregated_data[aggregated_data['slice_store_type'] == 'File-Backed']

for param in file_backed_config_params:
    if param in file_backed_data.columns:
        plt.figure(figsize=(14, 6))
        sns.lineplot(data=file_backed_data, x=param, y='throughput_data', errorbar=None, color='blue')
        plt.title(f'Effect of {param} on Throughput (File-Backed Only)')
        plt.xlabel(param)
        plt.ylabel('Average Throughput')
        plt.show()
    else:
        print(f"Parameter {param} not found in the dataset.")
