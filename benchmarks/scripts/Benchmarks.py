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


# Define helper functions
def chunk_list(list, chunk_size):
    for i in range(0, len(list), chunk_size):
        yield list[i:i + chunk_size]


def filter_by_config(data, config):
    mask = pd.Series([True] * len(data))
    for k, v in config.items():
        mask &= data[k] == v
    return data[mask]


def filter_by_default_values_except_param(data, param):
    other_params = [p for p in all_config_params if p != param]
    mask = pd.Series(True, index=data.index)
    for p in other_params:
        mask &= data[p].isin(default_param_values[p])
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


def convert_units(data, col, unit='B'):
    orders = ['', 'K', 'M', 'G', 'T']
    factor = 1000.0

    # Get max value for metric
    max_val = data[col].max()

    order_idx = 0
    while max_val >= factor and order_idx < len(orders) - 1:
        max_val /= factor
        order_idx += 1

    scaled_data = data.copy()
    scaled_data[col] = scaled_data[col] / (factor ** order_idx)
    return scaled_data, orders[order_idx] + unit


def add_categorical_param(data, param, new_param):
    # Add categorical version, sorted and ordered
    data = data.copy()
    data[new_param] = pd.Categorical(data[param], categories=sorted(data[param].unique()), ordered=True)
    return data


def expand_constant_params_for_groups(data, param, group, exclude_group_val=[]):
    rows_to_add = []

    # Get all unique values of param in the dataset
    all_max_mem_values = data[param].unique()

    for group_value in data[group].unique():
        if group_value in exclude_group_val:
            continue
        group_data = data[data[group] == group_value]
        unique_mem_values = group_data[param].unique()

        # Only expand if there's just one unique value
        if len(unique_mem_values) == 1:
            # Value we want to "spread" across all param settings
            for new_mem in all_max_mem_values:
                if new_mem == unique_mem_values[0]:
                    continue  # Don't duplicate the existing one
                # Create copies of the existing rows with updated param
                duplicated_rows = group_data.copy()
                duplicated_rows[param] = new_mem
                rows_to_add.append(duplicated_rows)

    if rows_to_add:
        # Concatenate all new rows with the original data
        data = pd.concat([data] + rows_to_add, ignore_index=True)

    return data


def add_query_fig_text(labels, adjust_bottom, text_y):
    query_ids = [lbl.split(' | ')[1] for lbl in labels]
    mapping_text = "\n".join([f"{v}: {k}" for k, v in query_mapping.items() if v in query_ids])
    plt.tight_layout()
    plt.subplots_adjust(bottom=adjust_bottom)
    plt.figtext(0.0, text_y, mapping_text, wrap=True, ha='left', fontsize=9)


def add_min_max_labels(data, metric, ax, param, y_offset, color):
    max_idx = data[metric].idxmax()
    min_idx = data[metric].idxmin()
    max_row = data.loc[max_idx]
    min_row = data.loc[min_idx]

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


def add_min_max_labels_per_group(data, group, metric, ax, param):
    y_offsets = [0.02, -0.02, 0.04, -0.04]

    # Get the color used for each group line from the legend
    legend_handles, legend_labels = ax.get_legend_handles_labels()
    color_map = dict(zip(legend_labels, [handle.get_color() for handle in legend_handles]))

    # Add labels for min and max values for each group
    grouped = data.groupby([group, param])[metric].mean().reset_index()
    for i, group_type in enumerate(grouped[group].unique()):
        # Vertical offsets based on y-axis value range
        y_offset = y_offsets[i % len(y_offsets)] * (grouped[metric].max() - grouped[metric].min())
        color = color_map.get(group_type, 'black')

        sub = grouped[grouped[group] == group_type]
        add_min_max_labels(sub, metric, ax, param, y_offset, color)


def find_default_values_for_params(min_support_ratio=0.99):
    likely_defaults = {}

    for param in all_config_params:
        other_params = [p for p in all_config_params if p != param]

        # Sum up the numbers of unique values across all other parameters for each value of this parameter
        grouped = df.groupby(param)[other_params].nunique().sum(axis=1)

        # Get most widely used value and all other values that occur with a similar frequency
        max_usage = grouped.max()
        defaults = grouped[grouped >= max_usage * min_support_ratio].index.tolist()

        likely_defaults[param] = defaults

    return likely_defaults

dtypes = {
    "timestamp_increment": "UInt64",
    "ingestion_rate": "UInt64",
    "match_rate": "UInt64",
    "number_of_worker_threads": "UInt64",
    "buffer_size_in_bytes": "UInt64",
    "page_size": "UInt64",
    "lower_memory_bound": "UInt64",
    "upper_memory_bound": "UInt64",
    "file_descriptor_buffer_size": "UInt64",
    "max_num_watermark_gaps": "UInt64",
    "max_num_sequence_numbers": "UInt64",
    "min_read_state_size": "UInt64",
    "min_write_state_size": "UInt64",
    "file_operation_time_delta": "UInt64",
    "file_layout": "str",
    "with_prediction": "str",
    "watermark_predictor_type": "str",
    "query": "str"
}

# Load the CSV file
df = pd.read_csv("data/amd/2025-06-17_18-31-03/combined_benchmark_statistics.csv")
# df = pd.read_csv("data/amd/2025-06-17_18-31-03/combined_benchmark_statistics.csv", dtype=dtypes)
for col in df.select_dtypes(include='float'):
    if not (df[col] % 1 == 0).all():
        print(f"⚠️ Column '{col}' contains non-integer float values!")
#print(df.select_dtypes(include='float').dtypes)
#print(df.select_dtypes(include='float').isna().sum())

print(df[['lower_memory_bound', 'upper_memory_bound']].describe())
print(df[['lower_memory_bound', 'upper_memory_bound']].max())

for col in ['lower_memory_bound', 'upper_memory_bound']:
    non_ints = df[col][df[col] % 1 != 0]
    if not non_ints.empty:
        print(f"⚠️ Column '{col}' has non-integer values:")
        print(non_ints)
    else:
        print(f"✅ Column '{col}' has only whole numbers.")
print(df[['lower_memory_bound', 'upper_memory_bound']].isna().sum())
max_uint64 = np.iinfo(np.uint64).max
for col in ['lower_memory_bound', 'upper_memory_bound']:
    too_large = df[col][df[col] > max_uint64]
    if not too_large.empty:
        print(f"⚠️ Column '{col}' has values exceeding UInt64 max:")
        print(too_large)

for col in ['lower_memory_bound', 'upper_memory_bound', 'max_num_sequence_numbers']:
    # First, assert all values are really whole numbers
    if not (df[col] % 1 == 0).all():
        raise ValueError(f"Column {col} contains non-integer float values!")

    # Convert safely: float → int → UInt64
    df[col] = df[col].astype(np.uint64)
#df['lower_memory_bound'] = df['lower_memory_bound'].fillna(0)
#df['upper_memory_bound'] = df['upper_memory_bound'].fillna(0)
#df = df[df['lower_memory_bound'].notna()]
#df = df[df['upper_memory_bound'].notna()]
#df['lower_memory_bound'] = df['lower_memory_bound'].astype('UInt64')
#df['upper_memory_bound'] = df['upper_memory_bound'].astype('UInt64')

# Define configuration parameters
shared_config_params = [
    'buffer_size_in_bytes', 'ingestion_rate', 'number_of_worker_threads', 'page_size', 'query', 'timestamp_increment',
    'match_rate'
]
file_backed_config_params = [
    'file_descriptor_buffer_size', 'file_layout', 'file_operation_time_delta', 'max_num_sequence_numbers',
    'min_read_state_size', 'min_write_state_size', 'max_num_watermark_gaps', 'watermark_predictor_type',
    'lower_memory_bound', 'upper_memory_bound', 'with_prediction'
]
all_config_params = shared_config_params + file_backed_config_params

# Find all default values for each config parameter
default_param_values = find_default_values_for_params()

# Find unique configs that both slice store types have in common
default_configs = df[df['slice_store_type'] == 'DEFAULT'][all_config_params].drop_duplicates()
file_backed_configs = df[df['slice_store_type'] == 'FILE_BACKED'][all_config_params].drop_duplicates()
common_configs = pd.merge(default_configs, file_backed_configs, on=all_config_params)
common_config_dicts = common_configs.to_dict(orient='records')

# Map long queries to short codes and sort by hue column values
unique_queries = df['query'].unique()
query_mapping = {q: f"Q{i}" for i, q in enumerate(unique_queries, start=1)}
df['query_id'] = df['query'].map(query_mapping)
df = df.sort_values(by=['timestamp_increment', 'query_id'], ascending=[True, True])

# Add a hue column with default params
df['hue'] = df['slice_store_type'] + ' | ' + df['query_id'] + ' | ' + df['timestamp_increment'].astype(str)


# %% Compare slice store types for different configs

def plot_config_comparison(data, configs, metric, label, config_id):
    param = 'config_id'
    matching_rows = []

    for i, config in enumerate(configs, start=config_id + 1):
        # Collect all rows for each config and map to short codes
        subset = filter_by_config(data, config).copy()
        subset[param] = f'C{i}'
        matching_rows.append(subset)

    combined = pd.concat(matching_rows, ignore_index=True)
    data_scaled, unit = convert_units(combined, metric)

    plt.figure(figsize=(14, 6))
    sns.barplot(data=data_scaled, x=param, y=metric, hue='slice_store_type', errorbar='sd')

    # Add configs below
    config_text = "\n".join(
        [f"C{i}: " + ", ".join(f"{k}={v}" for k, v in config.items() if k in shared_config_params) for i, config in
         enumerate(configs, start=config_id + 1)])
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.5)
    plt.figtext(0.0, -0.9, config_text, wrap=True, ha='left', fontsize=9)

    plt.title(f'Effect of Configs on {label}')
    plt.ylabel(f'{label} ({unit})')
    plt.xlabel('config')
    plt.legend(title='Slice Store Type')
    plt.show()


chunk_size = 25
for i, config_chunk in enumerate(chunk_list(common_config_dicts, chunk_size)):
    print(f"Plotting config chunk {i + 1} (configs {(i * 25) + 1}-{min((i + 1) * 25, len(common_config_dicts))} of "
          f"{len(common_config_dicts)})")
    plot_config_comparison(df, config_chunk, 'throughput_data', 'Throughput / sec', i * chunk_size)
    plot_config_comparison(df, config_chunk, 'memory', 'Memory', i * chunk_size)


# %% Compare slice store types for different configs over time

def interpolate_and_align_runs(data, param, metric, group_cols, output_points=100, extrapolate_to=(0, 8000)):
    interpolated_dfs = []

    # Determine global range across all runs
    all_min = extrapolate_to[0]
    all_max = extrapolate_to[1] or data[param].max()
    x_new = np.linspace(all_min, all_max, output_points)

    for _, group in data.groupby(group_cols):
        group_sorted = group.sort_values(by=param)
        x = group_sorted[param].values
        y = group_sorted[metric].values

        # Skip groups that can't be interpolated
        if len(x) < 2 or np.any(np.isnan(y)):
            continue

        try:
            f_interp = interp1d(x, y, kind='linear', bounds_error=False, fill_value='extrapolate')
            y_interp = f_interp(x_new)
            df_interp = pd.DataFrame({param: x_new, metric: y_interp})
            df_interp['slice_store_type'] = group['slice_store_type'].iloc[0]
            interpolated_dfs.append(df_interp)
        except Exception as e:
            print(f"Interpolation failed for a run: {e}")
            continue

    return pd.concat(interpolated_dfs, ignore_index=True) if interpolated_dfs else pd.DataFrame()


def plot_time_comparison(data, config, metric, label, plot):
    param = 'window_start_normalized'
    unit = ''

    filtered_data = filter_by_config(data, config)
    #filtered_data = filtered_data[filtered_data['slice_store_type'] == 'DEFAULT']
    if filtered_data.empty:
        print(f'No data available for config: {config}.')
        return

    #if interpolated_data.empty:
    #    print(f"No valid interpolated data for config: {config}")
    #    return

    if plot == 1:
        normalized_data = normalize_time_per_groups(filtered_data, param, ['slice_store_type', 'dir_name'], 'window_start')
        data, unit = convert_units(normalized_data, metric)
        #data, unit = [normalized_data, 'B']
    if plot == 2:
        param = 'window_start'
        min_val = filtered_data[param].min()
        filtered_data[param] = filtered_data[param] - min_val
        interpolated_data = interpolate_and_align_runs(filtered_data, param, metric, ['slice_store_type', 'dir_name'], extrapolate_to=(filtered_data[param].min(), filtered_data[param].max()))
        #interpolated_data[param] = interpolated_data[param] / 1000
        #shifted_data = shift_time_per_groups(interpolated_data, param, ['slice_store_type', 'dir_name'], 'window_start')
        data, unit = convert_units(interpolated_data, metric)
        #data, unit = [shifted_data, 'B']
    if plot == 3:
        interpolated_data = interpolate_and_align_runs(filtered_data, param, metric, ['slice_store_type', 'dir_name'])
        normalized_data = normalize_time_per_groups(interpolated_data, param, 'slice_store_type', param)
        data, unit = convert_units(normalized_data, metric)
        #data, unit = [normalized_data, 'B']
    if plot == 4:
        interpolated_data = interpolate_and_align_runs(filtered_data, param, metric, ['slice_store_type', 'dir_name'], extrapolate_to=(filtered_data[param].min(), filtered_data[param].max()))
        #interpolated_data[param] = interpolated_data[param] / 1000
        #shifted_data = shift_time_per_groups(interpolated_data, param, ['slice_store_type', 'dir_name'], param)
        data, unit = convert_units(interpolated_data, metric)
        #data, unit = [shifted_data, 'B']

    plt.figure(figsize=(14, 6))
    ax = sns.lineplot(data=data, x=param, y=metric, hue='slice_store_type', errorbar=None, marker='o')  # TODO errorbar='sd'

    # Add labels for min and max values of metric for each slice store type
    add_min_max_labels_per_group(data, 'slice_store_type', metric, ax, param)

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


def plot_test(data, config, metric, label):
    param = 'window_start_normalized'

    filtered_data = filter_by_config(data, config)
    #filtered_data = filtered_data[filtered_data['slice_store_type'] == 'DEFAULT']
    if filtered_data.empty:
        print(f'No data available for config: {config}.')
        return

    for _, data in filtered_data.groupby('dir_name'):
        data, unit = convert_units(data, metric)

        plt.figure(figsize=(14, 6))
        ax = sns.lineplot(data=data, x=param, y=metric, hue='slice_store_type', errorbar=None, marker='o')
    
        # Add labels for min and max values of metric for each slice store type
        add_min_max_labels_per_group(data, 'slice_store_type', metric, ax, param)
    
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
    #plot_test(df, config, 'throughput_data', 'Throughput / sec')
    for plot in plots:
        plot_time_comparison(df, config, 'throughput_data', 'Throughput / sec', plot)
        #plot_time_comparison(df, config, 'memory', 'Memory', plot)


# %% Shared parameter plots

def plot_shared_params(data, param, metric, hue, label, legend):
    data = data[data['buffer_size_in_bytes'] <= 131072]
    data = data[data['page_size'] <= 131072]
    data = data[data['ingestion_rate'] <= 10000]
    data = data[data['timestamp_increment'] <= 10000]
    if param != 'match_rate':
        data = data[data['match_rate'] == 90]

    data = filter_by_default_values_except_param(data, param)
    data_scaled, metric_unit = convert_units(data, metric)

    if param == 'query':
        # Sort data by whether or not the query contained variable sized data
        data_scaled['var_sized_data'] = data_scaled['query'].str.contains('tcp_source4')
        data_scaled = data_scaled.sort_values(by=['query_id', 'var_sized_data'], ascending=[True, True])

        data_scaled['hue'] = data_scaled['slice_store_type'] + ' | ' + data_scaled['timestamp_increment'].astype(str)
        legend = 'Slice Store | Time Increment'
        param = 'query_id'
    if param == 'timestamp_increment':
        data_scaled['hue'] = data_scaled['slice_store_type'] + ' | ' + data_scaled['query_id']
        legend = 'Slice Store | Query'

    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(data_scaled[param]):
        data_scaled, param_unit = convert_units(data_scaled, param, '')
        ax = sns.lineplot(data=data_scaled, x=param, y=metric, hue=hue, errorbar='sd', marker='o')

        # Add labels for min and max values of metric for this param for each value of hue
        add_min_max_labels_per_group(data_scaled, hue, metric, ax, param)
    else:
        ax = sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue)

    # Add legend below
    if param == 'query_id':
        add_query_fig_text(' | ' + data_scaled['query_id'].unique(), 0.35, -0.65)
    else:
        add_query_fig_text(ax.get_legend_handles_labels()[1], 0.2, 0.0)

    plt.title(f'Effect of {param} on {label}')
    plt.xlabel(f'{param} ({param_unit})' if 'param_unit' in locals() and param_unit != '' else param)
    plt.ylabel(f'{label} ({metric_unit})')
    plt.legend(title=legend)
    plt.show()


print(f"number of queries: {len(df['query'].unique())}")
print(f"number of timestamp increments: {len(df['timestamp_increment'].unique())}")
for param in shared_config_params:
    plot_shared_params(df, param, 'throughput_data', 'hue', 'Throughput / sec', 'Slice Store | Query | Time Increment')
    plot_shared_params(df, param, 'memory', 'hue', 'Memory', 'Slice Store | Query | Time Increment')


# %% File-Backed-Only parameter plots

def plot_file_backed_params(data, param, metric, hue, label, color):
    # Select one specific default config
    # data = data[data['query'] == default_query]
    data = data[data['timestamp_increment'] == 1]
    data = data[data['match_rate'] == 90]

    data = data[data['file_descriptor_buffer_size'] <= 131072]
    data = data[data['file_operation_time_delta'] <= 100]
    data = data[data['num_watermark_gaps_allowed'] <= 100]
    #if param == 'max_memory_consumption' or hue == 'max_memory_consumption':
    #    data = data[data['max_memory_consumption'] <= 4294967296]
    if param == 'max_num_sequence_numbers':
        data = data[data['max_num_sequence_numbers'] <= 1000]

    data_scaled, metric_unit = convert_units(data, metric)

    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(data_scaled[param]):
        data_scaled, param_unit = convert_units(data_scaled, param, '')
        ax = sns.lineplot(data=data_scaled, x=param, y=metric, hue=hue, errorbar='sd', marker='o', palette=('dark:' + color))

        # Add labels for min and max values of metric for this param for each value of hue
        add_min_max_labels_per_group(data_scaled, hue, metric, ax, param)
    else:
        ax = sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue, palette=('dark:' + color))

        handles, labels = ax.get_legend_handles_labels()
        custom_labels = [f"{scaled_df[hue].iloc[0]:.1f} {label_unit}" for lbl in labels for scaled_df, label_unit in [convert_units(pd.DataFrame({hue: [float(lbl)]}), hue)]]
        ax.legend(handles, custom_labels, title='Max Memory')

    add_query_fig_text(ax.get_legend_handles_labels()[1], 0.2, 0.0)

    plt.title(f'Effect of {param} on {label} (File-Backed Only)')
    plt.xlabel(f'{param} ({param_unit})' if 'param_unit' in locals() and param_unit != '' else param)
    plt.ylabel(f'{label} ({metric_unit})')
    plt.legend(title='Slice Store | Query | Time Increment')
    plt.show()


file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
#file_backed_data = expand_constant_params_for_groups(file_backed_data, 'max_memory_consumption', 'memory_model')
#file_backed_data = add_categorical_param(file_backed_data, 'max_memory_consumption', 'max_memory_consumption_cat')
for param in file_backed_config_params:
    #if param == 'max_memory_consumption':
    #    hue = 'memory_model'
    #elif param == 'memory_model':
    #    hue = 'max_memory_consumption'
        #hue = 'max_memory_consumption_cat'
    plot_file_backed_params(file_backed_data, param, 'throughput_data', 'hue', 'Throughput / sec', 'blue')
    plot_file_backed_params(file_backed_data, param, 'memory', 'hue', 'Memory', 'orange')


# %% Memory-Model parameter plots for different memory budgets over time

