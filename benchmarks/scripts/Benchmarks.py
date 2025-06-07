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


def convert_metric_units(data, param, metric):
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    factor = 1024.0

    # Get max value for the current param and metric
    max_val = data.groupby(param)[metric].mean().max()

    unit_idx = 0
    while max_val >= factor and unit_idx < len(units) - 1:
        max_val /= factor
        unit_idx += 1

    scaled_data = data.copy()
    scaled_data[metric] = scaled_data[metric] / (factor ** unit_idx)
    return scaled_data, units[unit_idx]


# %%
# Normalize the window_start_normalized column for each configuration
def normalize_time(group):
    ts_min = group['window_start_normalized'].min()
    ts_max = group['window_start_normalized'].max()
    group['window_start_normalized'] = (group['window_start_normalized'] - ts_min) / (ts_max - ts_min)
    return group


# df = df.groupby('slice_store_type', group_keys=False).apply(normalize_time)

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


# %%
def plot_shared_params(data, param, metric, label):
    y_offsets = {'DEFAULT': 0.02, 'FILE_BACKED': -0.02}
    data_scaled, unit = convert_metric_units(data, param, metric)
    plt.figure(figsize=(14, 6))
    ax = sns.lineplot(data=data_scaled, x=param, y=metric, hue='slice_store_type', errorbar=None)
    plt.title(f'Effect of {param} on {label}')
    plt.xlabel(param)
    plt.ylabel(f"{label} ({unit})")
    plt.legend(title='Slice Store Type')

    # Get the color used for each slice_store_type line from the legend handles
    legend_handles, legend_labels = ax.get_legend_handles_labels()
    color_map = dict(zip(legend_labels, [handle.get_color() for handle in legend_handles]))

    # Get min and max values for each slice store type
    grouped = data_scaled.groupby(['slice_store_type', param])[metric].mean().reset_index()
    for slice_store_type in grouped['slice_store_type'].unique():
        sub = grouped[grouped['slice_store_type'] == slice_store_type]
        max_idx = sub[metric].idxmax()
        min_idx = sub[metric].idxmin()
        max_row = sub.loc[max_idx]
        min_row = sub.loc[min_idx]

        # Vertical offsets based on y-axis value range
        y_offset = y_offsets[slice_store_type] * (grouped[metric].max() - grouped[metric].min())
        color = color_map.get(slice_store_type, 'black')

        # Add labels for min and max values
        ax.text(
            max_row[param], max_row[metric] + y_offset,
            f"max: {max_row[metric]:.1f}",
            color=color, ha='center', va='bottom', fontweight='bold'
        )
        ax.text(
            min_row[param], min_row[metric] + y_offset,
            f"min: {min_row[metric]:.1f}",
            color=color, ha='center', va='top', fontweight='bold'
        )
    plt.show()


# Shared Parameter Plots
for param in shared_config_params:
    plot_shared_params(df, param, 'throughput_data', 'Throughput / sec')
    plot_shared_params(df, param, 'memory', 'Memory')


# %%
def plot_file_backed_params(data, param, metric, label, color):
    data_scaled, unit = convert_metric_units(data, param, metric)
    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(data_scaled[param]):
        ax = sns.lineplot(data=data_scaled, x=param, y=metric, errorbar=None, color=color)

        # Get min and max values
        grouped = data_scaled.groupby(param)[metric].mean().reset_index()
        max_idx = grouped[metric].idxmax()
        min_idx = grouped[metric].idxmin()
        max_row = grouped.loc[max_idx]
        min_row = grouped.loc[min_idx]

        # Add labels for min and max values
        ax.text(
            max_row[param], max_row[metric],
            f"max: {max_row[metric]:.1f}",
            color=color, ha='center', va='bottom', fontweight='bold'
        )
        ax.text(
            min_row[param], min_row[metric],
            f"min: {min_row[metric]:.1f}",
            color=color, ha='center', va='top', fontweight='bold'
        )
    else:
        sns.boxplot(data=data_scaled, x=param, y=metric, color=color)
    plt.title(f'Effect of {param} on {label} (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel(f"{label} ({unit})")
    plt.show()


# File-Backed-Only Parameter Plots
file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for param in file_backed_config_params:
    plot_file_backed_params(file_backed_data, param, 'throughput_data', 'Throughput / sec', 'blue')
    plot_file_backed_params(file_backed_data, param, 'memory', 'Memory', 'orange')
