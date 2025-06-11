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
from scipy.interpolate import interp1d


# Load the CSV file
df = pd.read_csv("data/amd/2025-06-05_10-15-06/new1_combined_benchmark_statistics.csv")

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

# Find unique configs that both slice store types have in common
all_config_params = shared_config_params + file_backed_config_params
default_configs = df[df['slice_store_type'] == 'DEFAULT'][all_config_params].drop_duplicates()
file_backed_configs = df[df['slice_store_type'] == 'FILE_BACKED'][all_config_params].drop_duplicates()
common_configs = pd.merge(default_configs, file_backed_configs, on=all_config_params)
common_config_dicts = common_configs.to_dict(orient='records')


# Define helper functions
def chunk_configs(configs, chunk_size):
    for i in range(0, len(configs), chunk_size):
        yield configs[i:i + chunk_size]


def filter_by_config(data, config):
    mask = pd.Series([True] * len(data))
    for k, v in config.items():
        mask &= data[k] == v
    return data[mask]


def normalize_time_per_groups(data, param, group_cols, time_col):
    group_min = data.groupby(group_cols)[time_col].transform('min')
    group_max = data.groupby(group_cols)[time_col].transform('max')
    data_normalized = data.copy()
    data_normalized[param] = (data_normalized[time_col] - group_min) / (group_max - group_min)
    return data_normalized


def shift_time_per_groups(data, param, group_cols, time_col):
    group_min = data.groupby(group_cols)[time_col].transform('min')
    data_shifted = data.copy()
    data_shifted[param] = data_shifted[time_col] - group_min
    return data_shifted


def convert_metric_units(data, param, metric):
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    factor = 1000.0

    # Get max value for metric
    max_val = data[metric].max()

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


# %% Compare slice store types for different configs over time

def plot_config_comparison(data, configs, metric, label, config_id):
    param = 'config_id'
    matching_rows = []

    for i, config in enumerate(configs, start=config_id + 1):
        # Collect all rows for each config and map to short codes
        subset = filter_by_config(data, config).copy()
        subset[param] = f'C{i}'
        matching_rows.append(subset)

    combined = pd.concat(matching_rows, ignore_index=True)
    data_scaled, unit = convert_metric_units(combined, param, metric)

    plt.figure(figsize=(14, 6))
    sns.barplot(data=data_scaled, x=param, y=metric, hue='slice_store_type', errorbar='sd')

    # Add configs below
    config_text = "\n".join(
        [f"C{i}: " + ", ".join(f"{k}={v}" for k, v in config.items() if k in shared_config_params) for i, config in
         enumerate(configs, start=config_id + 1)])
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.5)
    plt.figtext(0.0, -0.8, config_text, wrap=True, ha='left', fontsize=9)

    plt.title(f'Effect of Configs on {label}')
    plt.ylabel(f'{label} ({unit})')
    plt.xlabel('config')
    plt.legend(title='Slice Store Type')
    plt.show()


chunk_size = 25
for i, config_chunk in enumerate(chunk_configs(common_config_dicts, chunk_size)):
    print(f"Plotting config chunk {i + 1} (configs {(i * 25) + 1}-{min((i + 1) * 25, len(common_config_dicts))} of "
          f"{len(common_config_dicts)})")
    plot_config_comparison(df, config_chunk, 'throughput_data', 'Throughput / sec', i * chunk_size)
    plot_config_comparison(df, config_chunk, 'memory', 'Memory', i * chunk_size)


# %% Compare slice store types for different configs over time

def interpolate_and_average_runs(data, param, metric, group_cols, time_col, n_points=100):
    # Normalize time per run
    normalized_data = normalize_time_per_groups(data, param, group_cols, time_col)

    # Create a common time grid
    common_time_grid = np.linspace(0, 1, n_points)

    results = []

    for store_type, group_df in normalized_data.groupby('slice_store_type'):
        interpolated_runs = []

        for _, run_df in group_df.groupby('dir_name'):
            if run_df[param].nunique() < 2:
                continue  # Need at least two points to interpolate

            try:
                interp_func = interp1d(run_df[param], run_df[metric], kind='linear', bounds_error=False, fill_value='extrapolate')
                interpolated = interp_func(common_time_grid)
                interpolated_runs.append(interpolated)
            except Exception as e:
                print(f"Interpolation failed for a run: {e}")

        if interpolated_runs:
            avg_metric = np.mean(interpolated_runs, axis=0)
            temp_df = pd.DataFrame({
                param: common_time_grid,
                metric: avg_metric,
                'slice_store_type': store_type
            })
            results.append(temp_df)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def plot_time_comparison(data, config, metric, label, plot):
    param = 'window_start_normalized'
    filtered_data = filter_by_config(data, config)
    filtered_data = filtered_data[filtered_data['slice_store_type'] == 'DEFAULT']

    if not filtered_data.empty:
        averaged_data = interpolate_and_average_runs(
            filtered_data,
            param,
            metric,
            group_cols=['slice_store_type', 'dir_name'],
            time_col='window_start'
        )

        if averaged_data.empty:
            print(f"No valid interpolated data for config: {config}")
            return

        if plot == 1:
            normalized_data = normalize_time_per_groups(filtered_data, param, ['slice_store_type', 'dir_name'], 'window_start')
            data, unit = convert_metric_units(normalized_data, param, metric)
            #data, unit = [normalized_data, 'B']
        if plot == 2:
            param = 'window_start'
            filtered_data[param] = filtered_data[param] / 1000
            #shifted_data = shift_time_per_groups(filtered_data, param, ['slice_store_type', 'dir_name'], 'window_start')
            data, unit = convert_metric_units(filtered_data, param, metric)
            #data, unit = [shifted_data, 'B']
        if plot == 3:
            normalized_data = normalize_time_per_groups(filtered_data, param, ['slice_store_type', 'dir_name'], param)
            data, unit = convert_metric_units(normalized_data, param, metric)
            #data, unit = [normalized_data, 'B']
        if plot == 4:
            #filtered_data[param] = filtered_data[param] / 1000
            #shifted_data = shift_time_per_groups(filtered_data, param, ['slice_store_type', 'dir_name'], param)
            data, unit = convert_metric_units(filtered_data, param, metric)
            #data, unit = [shifted_data, 'B']

        plt.figure(figsize=(14, 6))
        ax = sns.lineplot(data=data, x=param, y=metric, hue='slice_store_type', errorbar=None)

        # Add labels for min and max values of metric for each slice store type
        add_min_max_labels_per_slice_store(data, metric, ax, param)

        # Add config below
        mapping_text = "\n".join([f"{k}: {v}" for k, v in config.items() if k in shared_config_params])
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.15)
        plt.figtext(0.0, -0.1, mapping_text, wrap=True, ha='left', fontsize=9)

        plt.title(f'Effect of {param} on {label}')
        plt.xlabel(f'{param} (sec)')
        plt.ylabel(f'{label} ({unit})')
        plt.legend(title='Slice Store Type')
        plt.show()
    else:
        print(f'No data available for the common configuration for {metric}.')


specific_query = 'SELECT * FROM (SELECT * FROM tcp_source) INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 ' \
                 'WINDOW SLIDING (timestamp, size 10000 ms, advance by 10000 ms) INTO csv_sink'
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

specific_rows = df[df['dir_name'] == '.cache/benchmarks/2025-06-05_10-15-06/SpillingBenchmarks_1749125641']
specific_values = specific_rows[all_config_params].drop_duplicates()
configs = specific_values.to_dict('records')

filtered_data = filter_by_config(df, configs[0]).sort_values(by="window_start")

#configs = [specific_config]
#configs = [d for d in common_config_dicts if d["query"] == specific_query]

plots = [1, 2, 3, 4]
print(f'number of common configs: {len(configs)}')
for config in configs:
    for plot in [4]:
        plot_time_comparison(df, config, 'throughput_data', 'Throughput / sec', plot)
        #plot_time_comparison(df, config, 'memory', 'Memory', plot)


# %% Shared Parameter Plots

def plot_shared_params(data, param, metric, label):
    data_scaled, unit = convert_metric_units(data, param, metric)

    if param == 'query':
        # Map long queries to short codes
        unique_queries = data_scaled[param].unique()
        query_mapping = {q: f"Q{i}" for i, q in enumerate(unique_queries, start=1)}
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


print(f"number of queries: {len(df['query'].unique())}")
for param in shared_config_params:
    plot_shared_params(df, param, 'throughput_data', 'Throughput / sec')
    plot_shared_params(df, param, 'memory', 'Memory')


# %% File-Backed-Only Parameter Plots

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


file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for param in file_backed_config_params:
    plot_file_backed_params(file_backed_data, param, 'throughput_data', 'Throughput / sec', 'blue')
    plot_file_backed_params(file_backed_data, param, 'memory', 'Memory', 'orange')
