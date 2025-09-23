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


import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d


SERVER = 'amd'
DATETIME = '2025-09-22_16-33-48'
FILE = 'combined_benchmark_statistics.csv'
# FILE = 'combined_slice_accesses.csv'
SLICE_ACCESSES = False


# Define helper functions
def chunk_list(list, chunk_size):
    for i in range(0, len(list), chunk_size):
        yield list[i:i + chunk_size]


def filter_by_config(data, config):
    mask = pd.Series(True, index=data.index)
    for k, v in config.items():
        mask &= data[k] == v
    return data[mask]


def filter_by_default_values_except_params(data, params):
    other_params = [p for p in all_config_params if p not in params]
    mask = pd.Series(True, index=data.index)
    for p in other_params:
        mask &= data[p].isin(default_param_values[p])
    return data[mask]


def normalize_time_per_groups(data, param, group_cols, time_col, param_factor=1.0):
    group_min = data.groupby(group_cols)[time_col].transform('min')
    group_max = data.groupby(group_cols)[time_col].transform('max')
    data_normalized = data.copy()
    data_normalized[param] = ((data_normalized[time_col] - group_min) / (group_max - group_min)) * param_factor
    return data_normalized


def shift_time_per_groups(data, param, group_cols, time_col):
    group_min = data.groupby(group_cols)[time_col].transform('min')
    data_shifted = data.copy()
    data_shifted[param] = data_shifted[time_col] - group_min
    return data_shifted


def cut_param_max_min_per_groups(data, param, group_cols):
    longest_start= max(group[param].min() for _, group in data.groupby(group_cols))
    shortest_end = min(group[param].max() for _, group in data.groupby(group_cols))
    return data[(data[param] >= longest_start) & (data[param] <= shortest_end)]


def convert_unit(value, unit='B', order_idx=3, factor=1000.0):
    orders = ['n', 'µ', 'm', '', 'K', 'M', 'G', 'T', 'P', 'E']

    order_idx_tmp = order_idx
    while value >= factor and order_idx_tmp < len(orders) - 1:
        value /= factor
        order_idx_tmp += 1

    if order_idx_tmp <= 7:
        return f"{value:.2f} {orders[order_idx_tmp] + unit}"
    else:
        return "infinite"


def convert_units(data, col, unit='B', order_idx=3, factor=1000.0):
    orders = ['n', 'µ', 'm', '', 'K', 'M', 'G', 'T', 'P', 'E']

    # Get max value for metric
    max_val = data[col].max()

    order_idx_tmp = order_idx
    while max_val >= factor and order_idx_tmp < len(orders) - 1:
        max_val /= factor
        order_idx_tmp += 1

    scaled_data = data.copy()
    scaled_data[col] = scaled_data[col] / (factor ** (order_idx_tmp - order_idx))
    return scaled_data, orders[order_idx_tmp] + unit


def convert_units_to_match_value(data, col, value, factor=1000.0):
    scaled_data = data.copy()
    max_val_col = scaled_data[col].max()
    while max_val_col > value:
        scaled_data[col] = scaled_data[col] / factor
        max_val_col = scaled_data[col].max()
    return scaled_data


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


def add_query_fig_text(labels, adjust_bottom, text_y, text_x=0.0):
    query_ids = [lbl.split(' | ')[1] for lbl in labels]
    mapping_text = '\n'.join([f'{v}: {k}' for k, v in query_mapping.items() if v in query_ids])
    plt.tight_layout()
    plt.subplots_adjust(bottom=adjust_bottom)
    plt.figtext(text_x, text_y, mapping_text, wrap=True, ha='left', fontsize=9)


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


def add_numeric_labels_per_hue(ax, data, hue, param, legend, spacing=0.0725):
    # Calculate the new y-axis limit to make space for labels
    y_min, y_max = ax.get_ylim()
    new_y_min = y_min - (0.05 * (y_max - y_min))
    ax.set_ylim(new_y_min, y_max)

    # Add numerical labels under each box
    num_hue_values = len(data[hue].unique())
    for i in range(len(data[param].unique())):
        for j in range(num_hue_values):
            x_pos = i + (j - (num_hue_values - 1) / 2) * spacing
            ax.text(x_pos, y_min, f"{j + 1}", ha='center', va='top', fontsize=10)

    # Customize the legend to include numerical labels
    handles, labels = ax.get_legend_handles_labels()
    new_labels = [f"{j + 1}: {label}" for j, label in enumerate(labels)]
    ax.legend(handles, new_labels, title='ID: ' + legend)


def convert_and_get_memory_bounds(data, max_bound, max_scaled_bound, metric):
    vals = []
    for col in ['lower_memory_bound', 'upper_memory_bound']:
        data_filtered = data[(data[col] <= max_bound) & (data[col] > 0)]
        data_filtered = convert_units_to_match_value(data_filtered, col, max_scaled_bound)
        vals += data_filtered[col].unique().tolist()
    return sorted(set(vals))


def find_default_values_for_params(data, min_support_ratio=0.9):
    likely_defaults = {}

    for param in all_config_params:
        other_params = [p for p in all_config_params if p != param]

        # Sum up the numbers of unique values across all other parameters for each value of this parameter
        grouped = data.groupby(param)[other_params].nunique().sum(axis=1)

        # Get most widely used value and all other values that occur with a similar frequency
        max_usage = grouped.max()
        defaults = grouped[grouped >= max_usage * min_support_ratio].index.tolist()

        likely_defaults[param] = defaults

    return likely_defaults


def interpolate_and_align_per_groups(data, param, metric, group_cols, output_points=100, fill_value='extrapolate', extrapolate_to=(0, 8000)):
    interpolated_dfs = []

    # Determine global range across all runs
    all_min = extrapolate_to[0] or data[param].min()
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
            f_interp = interp1d(x, y, kind='linear', bounds_error=False, fill_value=fill_value)
            y_interp = f_interp(x_new)
            df_interp = pd.DataFrame({param: x_new, metric: y_interp})
            for col in group_cols:
                df_interp[col] = group[col].unique()[0]
            interpolated_dfs.append(df_interp)
        except Exception as e:
            print(f'Interpolation failed for a run: {e}')
            continue

    return pd.concat(interpolated_dfs, ignore_index=True) if interpolated_dfs else pd.DataFrame()


def downsample_data(data, cols, sample_interval=100):
    downsampled_dfs = []
    for name, group in data.groupby('dir_name'):
        bins = pd.cut(group['window_start_normalized'], np.arange(0, group['window_start_normalized'].max() + sample_interval, sample_interval), right=False)
        downsampled_group = group.groupby(bins, observed=False).agg({'throughput_data': 'mean', 'throughput_tuples': 'mean', 'memory': 'max'}).reset_index()
        downsampled_group['window_start_normalized'] = downsampled_group['window_start_normalized'].apply(lambda x: x.left)

        downsampled_group['dir_name'] = name
        for col in cols:
            downsampled_group[col] = group[col].iloc[0]
        downsampled_dfs.append(downsampled_group)
    return pd.concat(downsampled_dfs, ignore_index=True)


def valid_row(row):
    return pd.notna(row["last_write_nopred_end"]) and pd.notna(row["first_read_nopred_start"])


def compute_prediction_accuracy(row):
    if pd.notna(row['last_read_pred_end']) and row['last_read_pred_end'] >= row['first_read_nopred_start']:
        return False
    if pd.notna(row['last_write_pred_end']) and row['last_write_pred_end'] >= row['first_read_nopred_start']:
        return False
    # if pd.notna(row['last_write_pred_end']) and pd.notna(row['first_read_pred_start']):
    #     if row['last_write_pred_end'] >= row['first_read_pred_start']:
    #         return False

    return True


def compute_prediction_accuracy_2(row):
    if pd.notna(row['last_write_pred_end']) and pd.notna(row['first_read_pred_start']):
        if row['last_write_pred_end'] >= row['first_read_pred_start']:
            return False

    return True


def compute_prediction_precision(row):
    if pd.notna(row['last_write_pred_end']) and pd.notna(row['last_read_pred_end']):
        last_pred = max(row['last_write_pred_end'], row['last_read_pred_end'])
    elif pd.notna(row['last_write_pred_end']):
        last_pred = row['last_write_pred_end']
    elif pd.notna(row['last_read_pred_end']):
        last_pred = row['last_read_pred_end']
    else:
        return np.nan

    return row['last_write_nopred_end'] - last_pred


def compute_prediction_precision_2(row):
    return row['first_read_nopred_start'] - row['last_write_nopred_end']


# Load the CSV file
dtypes = {
    'lower_memory_bound': 'UInt64',
    'upper_memory_bound': 'UInt64',
    'max_num_sequence_numbers': 'UInt64',
    'with_cleanup': 'str',
    'with_prediction': 'str'
}
df = pd.read_csv(os.path.join('data', SERVER, DATETIME, FILE), dtype=dtypes)

# Define configuration parameters
shared_config_params = [
    'buffer_size_in_bytes', 'ingestion_rate', 'number_of_worker_threads', 'page_size', 'query', 'timestamp_increment',
    'match_rate', 'batch_size'
]
file_backed_config_params = [
    'file_descriptor_buffer_size', 'prediction_time_delta', 'max_num_sequence_numbers', 'watermark_predictor_type',
    'min_read_state_size', 'min_write_state_size', 'max_num_watermark_gaps', 'file_layout', 'max_num_file_descriptors',
    'lower_memory_bound', 'upper_memory_bound', 'with_cleanup', 'with_prediction', 'num_buffers_per_worker'
]
watermark_predictor_config_params = [
    'prediction_time_delta', 'min_read_state_size', 'min_write_state_size'
]
all_config_params = shared_config_params + file_backed_config_params

# Find all default values for each config parameter
default_param_values = find_default_values_for_params(df)

# Find unique configs that both slice store types have in common
default_configs = df[df['slice_store_type'] == 'DEFAULT'][all_config_params].drop_duplicates()
file_backed_configs = df[df['slice_store_type'] == 'FILE_BACKED'][all_config_params].drop_duplicates()
common_configs = pd.merge(default_configs, file_backed_configs, on=all_config_params)
common_config_dicts = common_configs.to_dict(orient='records')

if SLICE_ACCESSES:
    # Filter out invalid rows
    print(f'Percentage of invalid rows: {int(100 * (len(df) - len(df[df.apply(valid_row, axis=1)])) / len(df))}%')
    df_old = df.copy()
    df = df[df.apply(valid_row, axis=1)]
    default_param_values_2 = find_default_values_for_params(df)
    
    # Compute accuracy of predictors
    df['prediction_accuracy'] = df.apply(compute_prediction_accuracy, axis=1)
    df['prediction_accuracy_2'] = df.apply(compute_prediction_accuracy_2, axis=1)
    
    # Compute precision of predictors
    df['prediction_precision'] = df.apply(compute_prediction_precision, axis=1)
    df['prediction_precision_2'] = df.apply(compute_prediction_precision_2, axis=1)

# Sort by all hue values
df = df.sort_values(by=['slice_store_type', 'timestamp_increment', 'query', 'max_num_watermark_gaps', 'max_num_sequence_numbers', 'prediction_time_delta', 'watermark_predictor_type', 'lower_memory_bound', 'upper_memory_bound'], ascending=[True, True, True, True, True, True, True, True, True])
# queries = df['query'].drop_duplicates().tolist()
# for query in queries:
#     print(query)

# Map long queries to short codes
query_mapping = {q: f'Q{i}' for i, q in enumerate(df['query'].unique(), start=1)}
df['query_id'] = df['query'].map(query_mapping)

# Add a hue column with default params
df['shared_hue'] = df['slice_store_type'] + ' | ' + df['query_id'] + ' | ' + df['timestamp_increment'].astype(str)
df['file_backed_hue'] = df['query_id'] + ' | ' + df['timestamp_increment'].astype(str)
df['watermark_predictor_hue'] = df['max_num_watermark_gaps'].astype(str) + ' | ' + df['max_num_sequence_numbers'].astype(str)
df['accuracy_precision_hue'] = df['max_num_watermark_gaps'].astype(str) + ' | ' + df['max_num_sequence_numbers'].astype(str) + ' | ' + df['prediction_time_delta'].astype(str)
df['memory_bounds_hue'] = df['lower_memory_bound'].astype(str) + ' | ' + df['upper_memory_bound'].astype(str)

downsampled_df = downsample_data(df, all_config_params + ['slice_store_type', 'query_id'])

# %% Compare slice store types for different configs

def plot_config_comparison(data, configs, metric, label, config_id):
    data = data[data['page_size'] <= 131072]

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
    config_text = '\n'.join(
        [f'C{i}: ' + ', '.join(f'{k}={v}' for k, v in config.items() if k in shared_config_params) for i, config in
         enumerate(configs, start=config_id + 1)])
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.5)
    plt.figtext(0.0, -0.9, config_text, wrap=True, ha='left', fontsize=9)

    plt.title(f'Effect of Configs on {label}')
    plt.ylabel(f'{label} [{unit}]')
    plt.xlabel('config')
    plt.legend(title='Slice Store Type')
    plt.show()


chunk_size = 25
for i, config_chunk in enumerate(chunk_list(common_config_dicts, chunk_size)):
    print(f'Plotting config chunk {i + 1} (configs {(i * 25) + 1}-{min((i + 1) * 25, len(common_config_dicts))} of '
          f'{len(common_config_dicts)})')
    plot_config_comparison(df, config_chunk, 'throughput_data', 'Throughput / sec', i * chunk_size)
    plot_config_comparison(df, config_chunk, 'memory', 'Memory', i * chunk_size)


# %% Compare slice store types for different configs over time

def plot_time_comparison(data, config, metric, hue, label, legend, interpolate=False):
    param = 'window_start_normalized'

    #data = filter_by_config(data, config)
    data_scaled, metric_unit = convert_units(data, metric)

    if interpolate:
        normalized_data = normalize_time_per_groups(data_scaled, param, ['dir_name'], param)
        interpolated_data = interpolate_and_align_per_groups(normalized_data, param, metric, ['slice_store_type', 'dir_name'], extrapolate_to=(normalized_data[param].min(), normalized_data[param].max()))
        data, param_unit = convert_units(interpolated_data, param, 'normalised')
    else:
        filtered_data = cut_param_max_min_per_groups(data_scaled, param, ['dir_name'])
        shifted_data = shift_time_per_groups(filtered_data, param, ['dir_name'], param)
        data, param_unit = convert_units(shifted_data, param, 's', 2)

    plt.figure(figsize=(14, 9))
    #plt.figure(figsize=(14, 6))
    ax = sns.lineplot(data=data, x=param, y=metric, hue=hue, errorbar=None, marker=None)

    # Add labels for min and max values of metric for this param for each value of hue
    add_min_max_labels_per_group(data, hue, metric, ax, param)

    # Add config below
    mapping_text = '\n'.join([f'{k}: {v}' for k, v in config.items() if k in all_config_params])
    plt.figtext(0.01, 0.015, mapping_text, wrap=True, ha='left', fontsize=9)
    #add_query_fig_text([' | ' + lbl for lbl in data['file_backed_hue'].unique()], 0.2, 0.02, 0.01)

    param = 'Time'
    plt.yscale("log")
    plt.subplots_adjust(left=0.05, right=0.99, top=0.95, bottom=0.45)
    #plt.subplots_adjust(left=0.05, right=0.99, top=0.9, bottom=0.15)

    plt.title(f'Effect of {param} on {label}')
    #plt.title(f'Effect of {param} on {label} (File-Backed Only) with query_id={data["query_id"].unique()[0]} and timestamp_increment={data["timestamp_increment"].unique()[0]}')
    plt.xlabel(f'{param} [{param_unit}]' if 'param_unit' in locals() and param_unit != '' else param)
    plt.ylabel(f'{label} [{metric_unit}]' if metric_unit != '' else label)
    plt.legend(title=legend)
    plt.show()


default_param_values['lower_memory_bound'] = [0]
default_param_values['upper_memory_bound'] = [np.iinfo(np.uint64).max]


# %%
for timestamp_increment in default_param_values['timestamp_increment']:
    for query in default_param_values['query']:
        data = df[(df['lower_memory_bound'] <= df['upper_memory_bound']) & (df['slice_store_type'] == 'FILE_BACKED') & 
                  (df['timestamp_increment'] == timestamp_increment) & (df['query'] == query)]
        data = filter_by_default_values_except_params(data, ['lower_memory_bound', 'upper_memory_bound'])

        if len(data) == 0 or data['query_id'].unique()[0] != 'Q2' or timestamp_increment != 1:
            continue
        data = data[(data['lower_memory_bound'] > 1048576)] # & (data['lower_memory_bound'] < data['upper_memory_bound'].unique().max())]

        #print(f"Data Rows: {len(data['dir_name'].unique())}")
        #print(config)

        plot_time_comparison(data, {}, 'throughput_data', 'memory_bounds_hue', 'Throughput / sec', 'Lower Memory Bound | Upper Memory Bound', False)
        plot_time_comparison(data, {}, 'memory', 'memory_bounds_hue', 'Memory', 'Lower Memory Bound | Upper Memory Bound', False)


# %%
for lower_memory_bound in df['lower_memory_bound'].unique():
    for upper_memory_bound in df['upper_memory_bound'].unique():
        if lower_memory_bound > upper_memory_bound:
            continue
        #data = df[((df['query_id'] == 'Q2') & (df['timestamp_increment'] == 1)) |
                  #((df['query_id'] == 'Q2') & (df['timestamp_increment'] == 1)) |
                  #((df['query_id'] == 'Q4') & (df['timestamp_increment'] == 1)) |
                  #((df['query_id'] == 'Q5') & (df['timestamp_increment'] == 1)) |
        #          ((df['query_id'] == 'Q5') & (df['timestamp_increment'] == 1)) |
                  #((df['query_id'] == 'Q8') & (df['timestamp_increment'] == 1)) |
        #          ((df['query_id'] == 'Q8') & (df['timestamp_increment'] == 1))]
        data = df[(df['query_id'] == 'Q2') | (df['query_id'] == 'Q5') | (df['query_id'] == 'Q8')]

        data_default = filter_by_default_values_except_params(data, [])
        data_default = data_default[data_default['slice_store_type'] == 'DEFAULT']
        data_memory_bound = filter_by_default_values_except_params(data, ['lower_memory_bound', 'upper_memory_bound', 'min_write_state_size'])
        data_memory_bound = data_memory_bound[data_memory_bound['slice_store_type'] == 'FILE_BACKED']
        data_memory_bound = data_memory_bound[data_memory_bound['lower_memory_bound'] == lower_memory_bound]
        data_memory_bound = data_memory_bound[data_memory_bound['upper_memory_bound'] == upper_memory_bound]
        data_memory_bound = data_memory_bound[data_memory_bound['min_write_state_size'] == 0] # 8388608]
        data = pd.concat([data_default, data_memory_bound], ignore_index=True)

        config = {param: ", ".join(map(str, data[param].unique().tolist())) for param in all_config_params}
        config['lower_memory_bound'] = lower_memory_bound
        config['upper_memory_bound'] = upper_memory_bound
        config['min_write_state_size'] = 0

        if len(data['dir_name'].unique()) < 2 or len(data['slice_store_type'].unique()) < 2:
            print(f"Data Rows: {len(data['dir_name'].unique())}")
            print(config)
            continue

        plot_time_comparison(data, config, 'throughput_data', 'shared_hue', 'Throughput / sec', 'Slice Store | Query | Time Increment', False)
        plot_time_comparison(data, config, 'memory', 'shared_hue', 'Memory', 'Slice Store | Query | Time Increment', False)


# %% Shared parameter plots

def plot_shared_params(data, param, metric, hue, label, legend):
    #data = data[data['buffer_size_in_bytes'] <= 131072]
    #data = data[data['page_size'] <= 131072]
    #data = data[data['ingestion_rate'] <= 10000]
    #data = data[data['timestamp_increment'] <= 10000]
    #data = data[data['batch_size'] < 10000]

    data = filter_by_default_values_except_params(data, [param])
    data_scaled, metric_unit = convert_units(data, metric)

    if param == 'query':
        # Sort data by whether or not the query contained variable sized data
        data_scaled['var_sized_data'] = data_scaled['query'].str.contains('tcp_source4')
        data_scaled = data_scaled.sort_values(by=['var_sized_data', 'query'], ascending=[True, True])

        data_scaled[hue] = data_scaled['slice_store_type'] + ' | ' + data_scaled['timestamp_increment'].astype(str)
        legend = 'Slice Store | Time Increment'
        param = 'query_id'
    if param == 'timestamp_increment':
        data_scaled[hue] = data_scaled['slice_store_type'] + ' | ' + data_scaled['query_id']
        legend = 'Slice Store | Query'

    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(data_scaled[param]):
        data_scaled, param_unit = convert_units(data_scaled, param, '')
        ax = sns.lineplot(data=data_scaled, x=param, y=metric, hue=hue, errorbar='sd', marker='o')
        ax.legend(title=legend)

        # Add labels for min and max values of metric for this param for each value of hue
        add_min_max_labels_per_group(data_scaled, hue, metric, ax, param)
    else:
        ax = sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue)

        # Create numeric labels for each hue value and add legend
        add_numeric_labels_per_hue(ax, data_scaled, hue, param, legend, 0.2)

    # Add legend below
    if param == 'query_id':
        add_query_fig_text(' | ' + data_scaled[param].unique(), 0.35, -0.65)
    else:
        add_query_fig_text(ax.get_legend_handles_labels()[1], 0.2, 0.0)

    plt.title(f'Effect of {param} on {label}')
    plt.xlabel(f'{param} [{param_unit}]' if 'param_unit' in locals() and param_unit != '' else param)
    plt.ylabel(f'{label} [{metric_unit}]' if metric_unit != '' else label)
    plt.show()


for param in shared_config_params:
    plot_shared_params(df, param, 'throughput_data', 'shared_hue', 'Throughput / sec', 'Slice Store | Query | Time Increment')
    plot_shared_params(df, param, 'memory', 'shared_hue', 'Memory', 'Slice Store | Query | Time Increment')


# %% File-Backed-Only parameter plots

def plot_file_backed_params(data, param, metric, hue, label, legend): #, color):
    # data = data[data['match_rate'] == 90]
    data = data[data['file_descriptor_buffer_size'] <= 131072]
    # data = data[data['prediction_time_delta'] <= 100]
    # data = data[data['max_num_watermark_gaps'] <= 100]
    #if param == 'max_memory_consumption' or hue == 'max_memory_consumption':
    #    data = data[data['max_memory_consumption'] <= 4294967296]
    if param == 'max_num_sequence_numbers':
        data = data[data['max_num_sequence_numbers'] <= 1000]

    data = filter_by_default_values_except_params(data, [param])
    data_scaled, metric_unit = convert_units(data, metric)

    plt.figure(figsize=(14, 6))
    if pd.api.types.is_numeric_dtype(data_scaled[param]):
        data_scaled, param_unit = convert_units(data_scaled, param, '')
        ax = sns.lineplot(data=data_scaled, x=param, y=metric, hue=hue, errorbar='sd', marker='o') #, palette=('dark:' + color))

        # Add labels for min and max values of metric for this param for each value of hue
        add_min_max_labels_per_group(data_scaled, hue, metric, ax, param)
    else:
        ax = sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue) #, palette=('dark:' + color))

        #handles, labels = ax.get_legend_handles_labels()
        #custom_labels = [f'{scaled_df[hue].iloc[0]:.1f} {label_unit}' for lbl in labels for scaled_df, label_unit in [convert_units(pd.DataFrame({hue: [float(lbl)]}), hue)]]
        #ax.legend(handles, custom_labels, title='Max Memory')

    # Add legend below
    add_query_fig_text([' | ' + lbl for lbl in ax.get_legend_handles_labels()[1]], 0.2, 0.0)

    plt.title(f'Effect of {param} on {label} (File-Backed Only)')
    plt.xlabel(f'{param} [{param_unit}]' if 'param_unit' in locals() and param_unit != '' else param)
    plt.ylabel(f'{label} [{metric_unit}]' if metric_unit != '' else label)
    plt.legend(title=legend)
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
    plot_file_backed_params(file_backed_data, param, 'throughput_data', 'file_backed_hue', 'Throughput / sec', 'Query | Time Increment') #, 'blue')
    plot_file_backed_params(file_backed_data, param, 'memory', 'file_backed_hue', 'Memory', 'Query | Time Increment') #, 'orange')


# %% Watermark-Predictor parameter plots

def plot_watermark_predictor_params(data, param, metric, hue, label, legend):
    fig, axes = plt.subplots(nrows=3, figsize=(14, 16), sharex=True)
    for i, predictor_value in enumerate(df['watermark_predictor_type'].unique()):
        data_filtered = data[data['watermark_predictor_type'] == predictor_value]
        ax = axes[i]

        data_filtered = filter_by_default_values_except_params(data_filtered, [param, 'max_num_watermark_gaps', 'max_num_sequence_numbers'])
        data_scaled, metric_unit = convert_units(data_filtered, metric)

        if pd.api.types.is_numeric_dtype(data_scaled[param]):
            if param == 'prediction_time_delta':
                data_scaled, param_unit = convert_units(data_scaled, param, 'μs')
            else:
                data_scaled, param_unit = convert_units(data_scaled, param)
            # sns.lineplot(data=data_scaled, x=param, y=metric, hue=hue, errorbar='sd', marker='o', ax=ax)
            sns.lineplot(data=data_scaled, x=param, y=metric, hue=hue, errorbar=None, marker='o', ax=ax)

            # Add labels for min and max values of metric for this param for each value of hue
            add_min_max_labels_per_group(data_scaled, hue, metric, ax, param)
        else:
            sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue, ax=ax)

        ax.set_title(f'watermark_predictor_type={data_scaled["watermark_predictor_type"].unique()[0]}')
        ax.set_xlabel(f'{param} [{param_unit}]' if 'param_unit' in locals() and param_unit != '' else param)
        ax.set_ylabel(f'{label} [{metric_unit}]' if metric_unit != '' else label)
        ax.legend(title=legend)

    # Add legend below
    add_query_fig_text([' | ' + lbl for lbl in data['file_backed_hue'].unique()], 0.2, 0.0)

    fig.suptitle(f'Effect of {param} on {label} (File-Backed Only) with query_id={data["query_id"].unique()[0]} and timestamp_increment={data["timestamp_increment"].unique()[0]}', fontsize=16)
    fig.tight_layout(rect=[0, 0.02, 1, 0.98])
    plt.show()


file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for param in watermark_predictor_config_params:
    for hue_value in df['file_backed_hue'].unique():
        data_per_hue = file_backed_data[file_backed_data['file_backed_hue'] == hue_value]
        if data_per_hue['query_id'].unique()[0] == 'Q3':
            plot_watermark_predictor_params(data_per_hue, param, 'throughput_data', 'watermark_predictor_hue', 'Throughput / sec', 'Watermark Gaps | Sequence Numbers')
            plot_watermark_predictor_params(data_per_hue, param, 'memory', 'watermark_predictor_hue', 'Memory', 'Watermark Gaps | Sequence Numbers')


# %% Watermark-Predictor accuracy and precision plots

def plot_watermark_predictor_accuracy_precision(data, param, metric, hue, label, legend):
    data = filter_by_default_values_except_params(data, [param, 'max_num_watermark_gaps', 'max_num_sequence_numbers', 'prediction_time_delta'])
    if metric == 'prediction_accuracy':
        data_scaled, metric_unit = convert_units(data, metric, '%')
    if metric == 'prediction_precision':
        data_scaled, metric_unit = convert_units(data, metric, 's', 0)

    plt.figure(figsize=(14, 6))
    ax = sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue)

    # Create numeric labels for each hue value and add legend
    add_numeric_labels_per_hue(ax, data_scaled, hue, param, legend, 0.0675)

    # Add legend below
    add_query_fig_text([' | ' + lbl for lbl in data['file_backed_hue'].unique()], 0.2, 0.0)

    plt.title(f'Effect of {param} on {label} (File-Backed Only) with query_id={data["query_id"].unique()[0]} and timestamp_increment={data["timestamp_increment"].unique()[0]}')
    plt.xlabel(param)
    plt.ylabel(f'{label} [{metric_unit}]' if metric_unit != '' else label)
    plt.show()


#df = df.head(4)
#accuracy = (
#    df.groupby("watermark_predictor_type")["prediction_accuracy"]
#      .mean()
#      .reset_index()
#)

file_backed_data = df[df['slice_store_type'] == 'FILE_BACKED']
for hue_value in df['file_backed_hue'].unique():
    data_per_hue = file_backed_data[file_backed_data['file_backed_hue'] == hue_value]
    # if data_per_hue['query_id'].unique()[0] == 'Q3':
    plot_watermark_predictor_accuracy_precision(data_per_hue, 'watermark_predictor_type', 'prediction_accuracy', 'accuracy_precision_hue', 'Prediction Accuracy', 'Watermark Gaps | Sequence Numbers | Time Delta')
    plot_watermark_predictor_accuracy_precision(data_per_hue, 'watermark_predictor_type', 'prediction_precision', 'accuracy_precision_hue', 'Prediction Presicion', 'Watermark Gaps | Sequence Numbers | Time Delta')


# %% Memory-Bounds plots for different memory models

def plot_memory_bounds_comparison(data, param, metric, hue, label, legend):
    data = filter_by_default_values_except_params(data, [param, 'lower_memory_bound', 'upper_memory_bound'])
    if data[metric].max() - data[metric].min() >= 1000000000:
        yscale = "log"
    data_scaled, metric_unit = convert_units(data, metric)

    plt.figure(figsize=(8, 5))
    ax = sns.boxplot(data=data_scaled, x=param, y=metric, hue=hue)

    if label == 'Memory Consumption':
        # Plot horizontal lines for lower and upper bounds
        #vals = convert_and_get_memory_bounds(data_scaled, data[metric].max(), data_scaled[metric].max(), metric)
        #for val in vals:
        #    plt.axhline(y=val, color='red', linestyle='-', linewidth=1, alpha=1, zorder=1, label=f'{val:.3f} {metric_unit}')

        # Add shaded regions for bounds
        for i, (group_name, group_data) in enumerate(data.groupby(param, sort=False)):
            if 0 < group_data['lower_memory_bound'].unique()[0] : # <= data[metric].max():
                vals = convert_and_get_memory_bounds(group_data, 2 * data[metric].max(), 2 * data_scaled[metric].max(), metric)
                ax.fill_between(x=[i-0.4, i+0.4], y1=vals[0], y2=vals[1], alpha=0.15, label=group_name.replace('\n', ' '))
    else:
        metric_unit += ' / sec'

    # Add legend below
    #add_query_fig_text([' | ' + lbl for lbl in data[hue].unique()], 0.25, 0.0)

    if 'yscale' in locals():    
        plt.yscale(yscale)
    plt.ylim(0, data_scaled[metric].max() * 1.05)
    plt.grid(axis='y', which='major', linestyle='-', linewidth=1)
    plt.title(f'Effect of Memory Bounds on {label}')
    plt.xlabel('(LowerMemoryBound, UpperMemoryBound)')
    plt.ylabel(f'{label} [{metric_unit}]' if metric_unit != '' else label)
    plt.legend(title=None) #legend)
    plt.show()


default_param_values['lower_memory_bound'] = [0]
default_param_values['upper_memory_bound'] = [np.iinfo(np.uint64).max]
default_param_values['with_prediction'] = ['True']

file_backed_data = downsampled_df[downsampled_df['slice_store_type'] == 'FILE_BACKED']
file_backed_data = file_backed_data[(file_backed_data['window_start_normalized'] >= 1000) & file_backed_data['window_start_normalized'] <= 4000]
file_backed_data = file_backed_data[~file_backed_data['query_id'].isin(['Q1', 'Q3', 'Q4', 'Q6', 'Q7', 'Q9'])]
file_backed_data = file_backed_data[file_backed_data['timestamp_increment'] == 1]
file_backed_data = file_backed_data[(file_backed_data['window_start_normalized'] >= 1000) & (file_backed_data['window_start_normalized'] <= 5000)]

memory_bounds = [(0, 0), (0, np.iinfo(np.uint64).max), (8388608, 16777216), (16777216, 33554432), (33554432, 67108864), (67108864, 134217728)] #, (np.iinfo(np.uint64).max, np.iinfo(np.uint64).max)]
file_backed_data = file_backed_data[file_backed_data.apply(lambda row: (row['lower_memory_bound'], row['upper_memory_bound']) in memory_bounds, axis=1)]
file_backed_data['memory_bounds'] = file_backed_data.apply(lambda row: f"({convert_unit(row['lower_memory_bound'])},\n{convert_unit(row['upper_memory_bound'])})", axis=1)

for min_write_state_size in [0]: #, 4096, 8388608]:
    default_param_values['min_write_state_size'] = [min_write_state_size]
    plot_memory_bounds_comparison(file_backed_data, 'memory_bounds', 'throughput_data', 'query_id', 'Throughput', 'Query')
    plot_memory_bounds_comparison(file_backed_data, 'memory_bounds', 'memory', 'query_id', 'Memory Consumption', 'Query')
