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


import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Load the CSV file
df = pd.read_csv("../data/amd/2025-06-05_10-15-06/combined_benchmark_statistics.csv")

# Define configuration parameters
shared_config_params = [
    'buffer_size_in_bytes', 'ingestion_rate', 'number_of_worker_threads',
    'page_size', 'query', 'timestamp_increment'
]

file_backed_config_params = [
    'file_descriptor_buffer_size', 'file_layout', 'file_operation_time_delta',
    'max_num_sequence_numbers', 'min_read_state_size', 'min_write_state_size',
    'num_watermark_gaps_allowed', 'watermark_predictor_type'
]


# Define helper functions
def normalize_time(data):
    ts_min = data['window_start'].min()
    ts_max = data['window_start'].max()
    data['window_start_normalized'] = (data['window_start'] - ts_min) / (ts_max - ts_min)
    return data


def filter_by_config(data, config):
    mask = pd.Series([True] * len(data))
    for k, v in config.items():
        mask &= data[k] == v
    return data[mask]


def convert_metric_units(data, param, metric):
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    factor = 1000.0

    # Get max value for the current param and metric
    max_val = data.groupby(param)[metric].mean().max()

    unit_idx = 0
    while max_val >= factor and unit_idx < len(units) - 1:
        max_val /= factor
        unit_idx += 1

    scaled_data = data.copy()
    scaled_data[metric] = scaled_data[metric] / (factor ** unit_idx)
    return scaled_data, units[unit_idx]


def add_min_max_labels(grouped, metric, ax, param, y_offset, color):
    max_idx = grouped[metric].idxmax()
    min_idx = grouped[metric].idxmin()
    max_row = grouped.loc[max_idx]
    min_row = grouped.loc[min_idx]

    # Add labels for min and max values
    ax.text(
        max_row[param], max_row[metric] + y_offset,
        f'MAX: {max_row[metric]:.1f}',
        color=color, ha='center', va='bottom'
    )
    ax.text(
        min_row[param], min_row[metric] + y_offset,
        f'MIN: {min_row[metric]:.1f}',
        color=color, ha='center', va='top'
    )


def add_min_max_labels_per_slice_store(data, metric, ax, param):
    y_offsets = {'DEFAULT': 0.02, 'FILE_BACKED': -0.02}

    # Get the color used for each slice_store_type line from the legend
    legend_handles, legend_labels = ax.get_legend_handles_labels()
    color_map = dict(zip(legend_labels, [handle.get_color() for handle in legend_handles]))

    # Add labels for min and max values for each slice store type
    grouped = data.groupby(['slice_store_type', param])[metric].mean().reset_index()
    for slice_store_type in grouped['slice_store_type'].unique():
        # Vertical offsets based on y-axis value range
        y_offset = y_offsets[slice_store_type] * (grouped[metric].max() - grouped[metric].min())
        color = color_map.get(slice_store_type, 'black')

        sub = grouped[grouped['slice_store_type'] == slice_store_type]
        add_min_max_labels(sub, metric, ax, param, y_offset, color)


def plot_config_comparison(data, configs, metric, label):
    data_scaled, unit = convert_metric_units(data, param, metric)
    # Step 1: Collect all rows for each config dict
    matching_rows = []

    for i, config in enumerate(configs):
        mask = pd.Series(True, index=data_scaled.index)
        for k, v in config.items():
            if k in data_scaled.columns:
                mask &= data_scaled[k] == v
        subset = data_scaled[mask].copy()
        subset['config_id'] = f'C{i+1}'  # Label for x-axis
        matching_rows.append(subset)

    # Step 2: Combine into a single DataFrame
    combined = pd.concat(matching_rows, ignore_index=True)

    plt.figure(figsize=(14, 6))
    sns.barplot(data=combined, x='config_id', y=metric, hue='slice_store_type', errorbar='sd')

    # Add configs below
    config_text = "\n".join([f"C{i+1}: " + ", ".join(f"{k}={v}" for k, v in config.items()) for i, config in enumerate(configs)])
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.45)
    plt.figtext(0.0, -1.155, config_text, wrap=True, ha='left', fontsize=9)

    plt.title(f'Effect of Configs on {label}')
    plt.ylabel(f'{label} ({unit})')
    plt.xlabel('config')
    plt.legend(title='Slice Store Type')
    plt.show()


def plot_time_comparison(data, config, metric, label):
    param = 'window_start_normalized'
    data_scaled, unit = convert_metric_units(data, param, metric)
    filtered_data = filter_by_config(data_scaled, config)

    if not filtered_data.empty:
        normalized_data = filtered_data.groupby('slice_store_type', group_keys=False).apply(normalize_time)

        plt.figure(figsize=(14, 6))
        ax = sns.lineplot(data=normalized_data, x=param, y=metric, hue='slice_store_type', errorbar=None)

        # Add labels for min and max values of metric for each slice store type
        add_min_max_labels_per_slice_store(normalized_data, metric, ax, param)

        # Add config below
        mapping_text = "\n".join([f"{k}: {v}" for k, v in config.items() if k in shared_config_params])
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.15)
        plt.figtext(0.0, -0.1, mapping_text, wrap=True, ha='left', fontsize=9)

        plt.title(f'Effect of {param} on {label}')
        plt.xlabel(param)
        plt.ylabel(f'{label} ({unit})')
        plt.legend(title='Slice Store Type')
        plt.show()
    else:
        print(f'No data available for the common configuration for {metric}.')


def plot_shared_params(data, param, metric, label):
    data_scaled, unit = convert_metric_units(data, param, metric)

    if param == 'query':
        # Map long queries to short codes
        unique_queries = data_scaled[param].unique()
        query_mapping = {q: f"Q{i+1}" for i, q in enumerate(unique_queries)}
        data_scaled[param] = data_scaled[param].map(query_mapping)

    plt.figure(figsize=(14, 6))
    ax = sns.lineplot(data=data_scaled, x=param, y=metric, hue='slice_store_type', errorbar=None)

    # Add labels for min and max values of metric for this param for each slice store type
    add_min_max_labels_per_slice_store(data_scaled, metric, ax, param)

    if param == 'query':
        # Add legend below
        mapping_text = "\n".join([f"{v}: {k}" for k, v in query_mapping.items()])
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.35)
        plt.figtext(0.0, -0.65, mapping_text, wrap=True, ha='left', fontsize=9)

    plt.title(f'Effect of {param} on {label}')
    plt.xlabel(param)
    plt.ylabel(f'{label} ({unit})')
    plt.legend(title='Slice Store Type')
    plt.show()


def plot_file_backed_params(data, param, metric, label, color):
    data_scaled, unit = convert_metric_units(data, param, metric)
    plt.figure(figsize=(14, 6))

    if pd.api.types.is_numeric_dtype(data_scaled[param]):
        ax = sns.lineplot(data=data_scaled, x=param, y=metric, errorbar=None, color=color)

        # Add labels for min and max values of metric for this param
        grouped = data_scaled.groupby(param)[metric].mean().reset_index()
        add_min_max_labels(grouped, metric, ax, param, 0, color)
    else:
        sns.boxplot(data=data_scaled, x=param, y=metric, color=color)

    plt.title(f'Effect of {param} on {label} (File-Backed Only)')
    plt.xlabel(param)
    plt.ylabel(f'{label} ({unit})')
    plt.show()

# %% Find unique configs that both slice store types have in common

specific_query = 'SELECT * FROM (SELECT * FROM tcp_source) INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size 10000 ms, advance by 10000 ms) INTO csv_sink'
specific_config = {
    'timestamp_increment': 1,
    'ingestion_rate': 0,
    'number_of_worker_threads': 4,
    'buffer_size_in_bytes': 4096,
    'page_size': 4096,
    'query': specific_query,
    'num_watermark_gaps_allowed': 10,
    'max_num_sequence_numbers': np.iinfo(np.uint64).max,
    'file_descriptor_buffer_size': 4096,
    'min_read_state_size': 0,
    'min_write_state_size': 0,
    'file_operation_time_delta': 0,
    'file_layout': 'NO_SEPARATION',
    'watermark_predictor_type': 'KALMAN'
}

all_config_params = shared_config_params + file_backed_config_params
default_configs = df[df['slice_store_type'] == 'DEFAULT'][all_config_params].drop_duplicates()
file_backed_configs = df[df['slice_store_type'] == 'FILE_BACKED'][all_config_params].drop_duplicates()
common_configs = pd.merge(default_configs, file_backed_configs, on=all_config_params)
common_configs = common_configs[common_configs['query'] == specific_query]
common_config_dicts = common_configs.to_dict(orient='records')
# common_config_dicts = [specific_config]


# %% Compare slice store types for different configs over time

print(f'number of common configs: {len(common_config_dicts)}')
plot_config_comparison(df, common_config_dicts, 'throughput_data', 'Throughput / sec')
plot_config_comparison(df, common_config_dicts, 'memory', 'Memory')


# %% Compare slice store types for different configs over time

print(f'number of common configs: {len(common_config_dicts)}')
for config in common_config_dicts:
    plot_time_comparison(df, config, 'throughput_data', 'Throughput / sec')
    plot_time_comparison(df, config, 'memory', 'Memory')


# %% Shared Parameter Plots

print(f"number of queries: {len(df['query'].unique())}")
for param in shared_config_params:
    plot_shared_params(df, param, 'throughput_data', 'Throughput / sec')
    plot_shared_params(df, param, 'memory', 'Memory')


# %% File-Backed-Only Parameter Plots

file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for param in file_backed_config_params:
    plot_file_backed_params(file_backed_data, param, 'throughput_data', 'Throughput / sec', 'blue')
    plot_file_backed_params(file_backed_data, param, 'memory', 'Memory', 'orange')
