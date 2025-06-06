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
    'buffer_size_in_bytes', 'ingestion_rate',
    'number_of_worker_threads', 'page_size',
    'query', 'timestamp_increment'
]

file_backed_config_params = [
    'file_descriptor_buffer_size', 'file_layout',
    'file_operation_time_delta', 'max_num_sequence_numbers',
    'min_read_state_size', 'min_write_state_size',
    'num_watermark_gaps_allowed', 'watermark_predictor_type'
]

# Normalize the window_start_normalized column for each configuration
def normalize_time(group):
    ts_min = group['window_start_normalized'].min()
    ts_max = group['window_start_normalized'].max()
    group['window_start_normalized'] = (group['window_start_normalized'] - ts_min) / (ts_max - ts_min)
    return group

df = df.groupby('slice_store_type', group_keys=False).apply(normalize_time)

# Visual Comparison Plot
def plot_comparison(data, metric):
    plt.figure(figsize=(14, 6))

    # Get unique configurations for both slice store types
    default_configs = data[data['slice_store_type'] == 'DEFAULT'][shared_config_params].drop_duplicates()
    file_backed_configs = data[data['slice_store_type'] == 'FILE_BACKED'][shared_config_params].drop_duplicates()

    # Find a common configuration
    common_config = pd.merge(default_configs, file_backed_configs, on=shared_config_params)

    if not common_config.empty:
        common_config_dict = common_config.iloc[0].to_dict()

        # Filter data for the common configuration
        default_data = data[(data['slice_store_type'] == 'DEFAULT') & (data[shared_config_params] == pd.Series(common_config_dict)).all(axis=1)]
        file_backed_data = data[(data['slice_store_type'] == 'FILE_BACKED') & (data[shared_config_params] == pd.Series(common_config_dict)).all(axis=1)]

        if not default_data.empty and not file_backed_data.empty:
            sns.lineplot(data=default_data, x='window_start_normalized', y=metric, label=f'Default {metric}', errorbar=None)
            sns.lineplot(data=file_backed_data, x='window_start_normalized', y=metric, label=f'File-Backed {metric}', errorbar=None)
            plt.title(f'{metric} Comparison: Default vs. File-Backed')
            plt.xlabel('Normalized Time')
            plt.ylabel(metric)
            plt.legend()
            plt.show()
        else:
            print(f"No data available for the common configuration for {metric}.")
    else:
        print(f"No matching configurations found for both slice store types for {metric}.")

# Plot comparisons for both metrics
plot_comparison(df, 'throughput_data')
plot_comparison(df, 'memory')

# Shared Parameter Plots
for param in shared_config_params:
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=df, x=param, y='throughput_data', hue='slice_store_type', errorbar=None)
    plt.title(f'Effect of {param} on Throughput')
    plt.xlabel(param)
    plt.ylabel('Throughput')
    plt.legend(title='Slice Store Type')
    plt.show()

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=df, x=param, y='memory', hue='slice_store_type', errorbar=None)
    plt.title(f'Effect of {param} on Memory')
    plt.xlabel(param)
    plt.ylabel('Memory')
    plt.legend(title='Slice Store Type')
    plt.show()

# File-Backed Only Parameter Plots
file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for param in file_backed_config_params:
    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(file_backed_data[param]):
        sns.lineplot(data=file_backed_data, x=param, y='throughput_data', errorbar=None, color='blue')
    else:
        sns.boxplot(data=file_backed_data, x=param, y='throughput_data', color='blue')
    plt.title(f'Effect of {param} on Throughput (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel('Throughput')
    plt.show()

    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(file_backed_data[param]):
        sns.lineplot(data=file_backed_data, x=param, y='memory', errorbar=None, color='orange')
    else:
        sns.boxplot(data=file_backed_data, x=param, y='memory', color='orange')
    plt.title(f'Effect of {param} on Memory (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel('Memory')
    plt.show()
