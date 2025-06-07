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
# def plot_comparison(data, metric):
#     plt.figure(figsize=(14, 6))
#     all_config_params = shared_config_params + file_backed_config_params
#
#     # Get unique configurations for both slice store types
#     default_configs = df[df['slice_store_type'] == 'DEFAULT'][all_config_params].drop_duplicates()
#     file_backed_configs = df[df['slice_store_type'] == 'FILE_BACKED'][all_config_params].drop_duplicates()
#
#     # Find a common configuration
#     common_config = pd.merge(default_configs, file_backed_configs, on=all_config_params)
#
#     if not common_config.empty:
#         common_config_dict = common_config.iloc[0].to_dict()
#
#         # Filter data for the common configuration
#         config_mask = (df[all_config_params] == pd.Series(common_config_dict)).all(axis=1)
#         filtered = df[config_mask]
#
#         sns.lineplot(data=filtered, x='window_start_normalized', y=metric, hue='slice_store_type', errorbar=None)
#         plt.title(f'{metric} Comparison: Default vs. File-Backed')
#         plt.xlabel('Normalized Time')
#         plt.ylabel(metric)
#         plt.legend()
#         plt.show()
#     else:
#         print(f"No matching configurations found for both slice store types for {metric}.")

# Plot comparisons for both metrics
plot_comparison(df, 'throughput_data')
plot_comparison(df, 'memory')

y_offsets = {
    'DEFAULT': 0.02,
    'FILE_BACKED': -0.02
}

# Shared Parameter Plots
for param in shared_config_params:
    is_numeric = pd.api.types.is_numeric_dtype(df[param])
    plt.figure(figsize=(14, 6))
    ax = sns.lineplot(data=df, x=param, y='throughput_data', hue='slice_store_type', errorbar=None)
    plt.title(f'Effect of {param} on Throughput')
    plt.xlabel(param)
    plt.ylabel('Throughput')
    plt.legend(title='Slice Store Type')

    # Get the color used for each slice_store_type line from the legend handles
    legend_handles, legend_labels = ax.get_legend_handles_labels()
    color_map = dict(zip(legend_labels, [handle.get_color() for handle in legend_handles]))

    grouped = df.groupby(['slice_store_type', param])['throughput_data'].mean().reset_index()

    # Add min/max labels for each slice_store_type
    for slice_type in grouped['slice_store_type'].unique():
        sub = grouped[grouped['slice_store_type'] == slice_type]

        max_idx = sub['throughput_data'].idxmax()
        min_idx = sub['throughput_data'].idxmin()

        max_row = sub.loc[max_idx]
        min_row = sub.loc[min_idx]

        # Vertical offsets based on y-axis value range
        y_offset = y_offsets[slice_type] * (grouped['throughput_data'].max() - grouped['throughput_data'].min())
        color = color_map.get(slice_type, 'black')

        ax.text(
            max_row[param], max_row['throughput_data'] + y_offset,
            f"max: {max_row['throughput_data']:.1f}",
            color=color, ha='center', va='bottom', fontweight='bold'
        )
        ax.text(
            min_row[param], min_row['throughput_data'] + y_offset,
            f"min: {min_row['throughput_data']:.1f}",
            color=color, ha='center', va='top', fontweight='bold'
        )

    plt.show()

    plt.figure(figsize=(14, 6))
    ax = sns.lineplot(data=df, x=param, y='memory', hue='slice_store_type', errorbar=None)
    plt.title(f'Effect of {param} on Memory')
    plt.xlabel(param)
    plt.ylabel('Memory')
    plt.legend(title='Slice Store Type')

    legend_handles, legend_labels = ax.get_legend_handles_labels()
    color_map = dict(zip(legend_labels, [handle.get_color() for handle in legend_handles]))

    grouped = df.groupby(['slice_store_type', param])['memory'].mean().reset_index()

    # Add min/max labels for each slice_store_type
    for slice_type in grouped['slice_store_type'].unique():
        sub = grouped[grouped['slice_store_type'] == slice_type]

        max_idx = sub['memory'].idxmax()
        min_idx = sub['memory'].idxmin()

        max_row = sub.loc[max_idx]
        min_row = sub.loc[min_idx]

        y_offset = y_offsets[slice_type] * (grouped['memory'].max() - grouped['memory'].min())
        color = color_map.get(slice_type, 'black')

        ax.text(
            max_row[param], max_row['memory'] + y_offset,
            f"max: {max_row['memory']:.1f}",
            color=color, ha='center', va='bottom', fontweight='bold'
        )
        ax.text(
            min_row[param], min_row['memory'] + y_offset,
            f"min: {min_row['memory']:.1f}",
            color=color, ha='center', va='top', fontweight='bold'
        )

    plt.show()

# File-Backed Only Parameter Plots
file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for param in file_backed_config_params:
    is_numeric = pd.api.types.is_numeric_dtype(file_backed_data[param])
    plt.figure(figsize=(14, 6))
    if is_numeric:
        ax = sns.lineplot(data=file_backed_data, x=param, y='throughput_data', errorbar=None, color='blue')
        grouped = file_backed_data.groupby(param)['throughput_data'].mean().reset_index()
        max_idx = grouped['throughput_data'].idxmax()
        min_idx = grouped['throughput_data'].idxmin()
        max_row = grouped.loc[max_idx]
        min_row = grouped.loc[min_idx]

        ax.text(
            max_row[param], max_row['throughput_data'],
            f"max: {max_row['throughput_data']:.1f}",
            color='blue', ha='center', va='bottom', fontweight='bold'
        )
        ax.text(
            min_row[param], min_row['throughput_data'],
            f"min: {min_row['throughput_data']:.1f}",
            color='blue', ha='center', va='top', fontweight='bold'
        )
    else:
        sns.boxplot(data=file_backed_data, x=param, y='throughput_data', color='blue')
    plt.title(f'Effect of {param} on Throughput (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel('Throughput')
    plt.show()

    plt.figure(figsize=(14, 6))
    if is_numeric:
        ax = sns.lineplot(data=file_backed_data, x=param, y='memory', errorbar=None, color='orange')
        grouped = file_backed_data.groupby(param)['memory'].mean().reset_index()
        max_idx = grouped['memory'].idxmax()
        min_idx = grouped['memory'].idxmin()
        max_row = grouped.loc[max_idx]
        min_row = grouped.loc[min_idx]

        ax.text(
            max_row[param], max_row['memory'],
            f"max: {max_row['memory']:.1f}",
            color='orange', ha='center', va='bottom', fontweight='bold'
        )
        ax.text(
            min_row[param], min_row['memory'],
            f"min: {min_row['memory']:.1f}",
            color='orange', ha='center', va='top', fontweight='bold'
        )
    else:
        sns.boxplot(data=file_backed_data, x=param, y='memory', color='orange')
    plt.title(f'Effect of {param} on Memory (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel('Memory')
    plt.show()
