#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 16:16:05 2025

@author: nikla
"""

import os
import re
import glob
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Change this to the directory where your CSV files are located
data_dir = "data/03-02-4GB"
file_pattern = os.path.join(data_dir, "micro_benchmarks_SeparationTest_*.csv")
files = glob.glob(file_pattern)

# Regular expression to parse filenames
# micro_benchmarks_SeparationTest_<sep_method>_bufferSize_<buffer_size>_fileBufferSizePercent_<file_buffer_size>[ _keys_<keys> ].csv
pattern = re.compile(
    r"micro_benchmarks_SeparationTest_(?P<sep_method>.+)_bufferSize_(?P<buffer_size>\d+)_fileBufferSizePercent_(?P<file_buffer_size>\d+)(?:_keys_(?P<keys>.+))?\.csv"
)

# Container to hold data from all files
data_frames = []

# Process each file
for file in files:
    basename = os.path.basename(file)
    match = pattern.match(basename)
    if not match:
        print(f"Filename {basename} does not match the expected pattern. Skipping.")
        continue

    metadata = match.groupdict()
    sep_method = metadata['sep_method']
    buffer_size = int(metadata['buffer_size'])
    file_buffer_size = int(metadata['file_buffer_size'])
    keys = metadata.get('keys') if metadata.get('keys') is not None else "no_keys"

    # Read CSV file
    df = pd.read_csv(file, header=None)

    # Verify that we have the expected 17 columns
    if df.shape[1] != 17:
        print(f"File {basename} does not have 17 columns. Skipping.")
        continue

    df.columns = [f'col{i}' for i in range(1, 18)]

    # Rename columns to clarify their roles
    df = df.rename(columns={
        'col1': 'generate_data',
        'col2': 'write_exec_time',
        'col3': 'def_var_write',
        'col4': 'alloc_mem_write',
        'col5': 'open_file_write',
        'col6': 'write_buf_write',
        'col7': 'write_file_write',
        'col8': 'close_file_write',
        'col9': 'truncate_file',
        'col10': 'read_exec_time',
        'col11': 'def_var_read',
        'col12': 'alloc_mem_read',
        'col13': 'open_file_read',
        'col14': 'write_buf_read',
        'col15': 'write_mem_read',
        'col16': 'close_file_read',
        'col17': 'compare_data'
    })

    # Calculate time to write to file without memory allocation
    writing_time = df['write_exec_time'] - df['alloc_mem_write']
    df["writing_time"] = writing_time / 1000

    # Calculate time to read from file without memory allocation
    reading_time = df['read_exec_time'] - df['alloc_mem_read']
    df["reading_time"] = reading_time / 1000

    df["truncating_time"] = df['truncate_file'] / 1000

    # Attach Metadata to each row
    df["sep_method"] = sep_method
    df["buffer_size"] = buffer_size
    df["file_buffer_size"] = file_buffer_size
    df["keys"] = keys

    # Append the processed DataFrame
    data_frames.append(df)

# Combine all experiments from all files into one DataFrame
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
else:
    raise RuntimeError("No valid CSV files found!")

# Add total execution time
combined_df['execution_time'] = combined_df["writing_time"] + combined_df["reading_time"] + combined_df["truncating_time"]

# Calculate the percentage of execution time for defining variables
combined_df['def_var_write_percent'] = ((combined_df['def_var_write'] / 1000) / combined_df['writing_time']) * 100
combined_df['def_var_read_percent'] = ((combined_df['def_var_read'] / 1000) / combined_df['reading_time']) * 100

# Calculate the assumed percentage of creating pagedVector of defining variables for reading
combined_df["alloc_pv_read_percent"] = ((combined_df["def_var_read"] - combined_df["def_var_write"]) / combined_df['def_var_read']) * 100

# Sort the combined values
combined_df = combined_df.sort_values(["sep_method", "buffer_size", "file_buffer_size"])

#%% Remove outliers

multiplier = 1.5
filtered_outliers_groups = []
columns_to_clean = ["writing_time", "truncating_time", "reading_time"]
group_columns = ["sep_method", "buffer_size", "file_buffer_size", "keys"]

# Loop over each group and remove outliers
for group_name, group in combined_df.groupby(group_columns):
    mask = pd.Series(True, index=group.index)

    for col in columns_to_clean:
        Q1 = group[col].quantile(0.25)
        Q3 = group[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR

        # Create a mask for rows where the value is within the acceptable range
        mask &= group[col].between(lower_bound, upper_bound)

    filtered_outliers_groups.append(group[mask])
    
filtered_outliers_df = pd.concat(filtered_outliers_groups, ignore_index=True)

#%% General overview using stacked barplot

plot_df = filtered_outliers_df

# Choose three representative values for each
selected_buffer_sizes = [1024, 4096, 16384]
selected_file_buffer_sizes = [0, 100]
selected_keys = ["no_keys", "f0_f1_f2_f3_f4", "f0_f2_f4_f6_f8"]

# Filter the DataFrame for only these values:
subset1= plot_df[
    (plot_df['buffer_size'].isin(selected_buffer_sizes)) &
    (plot_df['file_buffer_size'].isin(selected_file_buffer_sizes) &
    (plot_df['keys'].isin(selected_keys)))
].copy()

#############################################
# Group and Aggregate Data
#############################################

# Group by all four configuration columns and compute the mean of each timing metric.
grouped = subset1.groupby(['sep_method', 'buffer_size', 'file_buffer_size', 'keys']).agg({
    'writing_time': 'mean',
    'truncating_time': 'mean',
    'reading_time': 'mean'
}).reset_index()

# Sort for consistency (first by sep_method, then buffer_size, then file_buffer, then keys)
grouped = grouped.sort_values(['sep_method', 'buffer_size', 'file_buffer_size', 'keys']).reset_index(drop=True)

#############################################
# Determine x Positions and Prepare Tiered Labels
#############################################

# We will create three tiers:
#   Tier 1 (top): sep_method
#   Tier 2 (middle): buffer_size
#   Tier 3 (bottom): file_buffer and keys (we combine these for the final tick labels)

positions = []           # x positions for each bar
tick_labels = []         # bottom-tier label (file_buffer & keys)
buffer_labels = {}       # For each (sep_method, buffer_size), store center x position
sep_labels = {}          # For each sep_method, store center x position

current_x = 0
# Iterate over sep_method groups.
for sep in grouped['sep_method'].unique():
    df_sep = grouped[grouped['sep_method'] == sep]
    sep_positions = []  # positions for this sep_method group
    # Within each sep_method, group by buffer_size.
    for buf in df_sep['buffer_size'].unique():
        df_sep_buf = df_sep[df_sep['buffer_size'] == buf]
        buf_positions = []  # positions for this buffer_size group
        # For each row (file_buffer, keys) in this group:
        for _, row in df_sep_buf.iterrows():
            positions.append(current_x)
            sep_positions.append(current_x)
            buf_positions.append(current_x)
            # Create a label combining file_buffer and keys.
            tick_labels.append(f"F:{int(row['file_buffer_size'])} K:{row['keys']}")
            current_x += 1
        # Leave a small gap after each buffer_size group.
        # Record the center position for the buffer_size label.
        buffer_labels[(sep, buf)] = np.mean(buf_positions)
        current_x += 0.5
    # Record the center position for the sep_method group.
    sep_labels[sep] = np.mean(sep_positions)
    # Leave a larger gap between sep_method groups.
    current_x += 1

#############################################
# Create the Stacked Bar Plot
#############################################

# Prepare the values for the three timing metrics.
# (Ensure that the order in 'grouped' matches the order of positions.)
writing = grouped['writing_time'].values
truncating = grouped['truncating_time'].values
reading = grouped['reading_time'].values

fig, ax = plt.subplots(figsize=(24, 8))
width = 0.8

# Plot the stacked bars.
bar1 = ax.bar(positions, writing, width, label='Writing Time')
bar2 = ax.bar(positions, truncating, width, bottom=writing, label='Truncating Time')
bar3 = ax.bar(positions, reading, width, bottom=writing+truncating, label='Reading Time')

# Set the bottom (tier 3) x-tick labels.
ax.set_xticks(positions)
ax.set_xticklabels(tick_labels, rotation=45, ha='right')

#############################################
# Add Tiered Labels Above the x-axis
#############################################

ymax = ax.get_ylim()[1]
y_offset = ymax * 0.02  # offset for text above the axis

# Tier 2: buffer_size labels (placed below the top tier but above the tick labels)
for (sep, buf), pos in buffer_labels.items():
    ax.text(pos, -y_offset*14, f"BufferSize: {int(buf)}", ha='center', va='top', fontsize=10, fontweight='medium')

# Tier 1: sep_method labels (placed at the very top)
for sep, pos in sep_labels.items():
    ax.text(pos, -y_offset*16, sep, ha='center', va='top', fontsize=12, fontweight='bold')

# Adjust the bottom margin to make room for the multi-tier labels.
plt.subplots_adjust(bottom=0.25)
ax.set_ylabel('Execution Time')
ax.set_title('Overview: Execution Times by Configuration\n(Selected BufferSizes, FileBufferSizes, and Keys)')
ax.legend()

plt.show()

#%% Impact of bufferSize and fileBufferSize on execution times

subset2 = plot_df[
    (plot_df['buffer_size'] != 131072) &
    (plot_df['file_buffer_size'] == 0) &
    (plot_df['keys'].isin(selected_keys))
].copy()

# Melt the DataFrame so that we have one column for phase and one for its execution time
melted2 = pd.melt(subset2,
                  id_vars=['buffer_size', 'sep_method', 'keys'],
                  value_vars=['writing_time', 'truncating_time', 'reading_time'],
                  var_name='phase',
                  value_name='time')

# Create a line plots
g2 = sns.relplot(
    data=melted2,
    x='buffer_size', y='time', hue='sep_method',
    col='phase', style='keys',
    kind='line', marker='o',
    facet_kws={'sharey': False, 'sharex': True},
    height=4, aspect=1.2,
    estimator=np.mean
)
g2.set_titles("{col_name}")
g2.fig.suptitle('Impact of buffer_size on Execution Times', y=1.03)
plt.show()

subset3 = plot_df[
    (plot_df['buffer_size'] == 4096) &
    (plot_df['keys'].isin(selected_keys))
].copy()

# Melt the DataFrame so that we have one column for phase and one for its execution time
melted3 = pd.melt(subset3,
                  id_vars=['file_buffer_size', 'sep_method', 'keys'],
                  value_vars=['writing_time', 'truncating_time', 'reading_time'],
                  var_name='phase',
                  value_name='time')

# Create a line plots
g3 = sns.relplot(
    data=melted3,
    x='file_buffer_size', y='time', hue='sep_method',
    col='phase', style='keys',
    kind='line', marker='o',
    facet_kws={'sharey': False, 'sharex': True},
    height=4, aspect=1.2,
    estimator=np.mean
)
g3.set_titles("{col_name}")
g3.fig.suptitle('Impact of file_buffer_size on Execution Times', y=1.03)
plt.show()

subset4 = plot_df[
    (plot_df['buffer_size'] == 4096) &
    (plot_df['file_buffer_size'] == 0)
].copy()

# Melt the DataFrame so that we have one column for phase and one for its execution time
melted4 = pd.melt(subset4,
                  id_vars=['keys', 'sep_method'],
                  value_vars=['writing_time', 'truncating_time', 'reading_time'],
                  var_name='phase',
                  value_name='time')

# Create a line plots
g4 = sns.relplot(
    data=melted4,
    x='keys', y='time', hue='sep_method',
    col='phase',
    kind='line', marker='o',
    facet_kws={'sharey': False, 'sharex': True},
    height=4, aspect=2,
    estimator=np.mean
)
for ax in g4.axes.flat:
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
g4.set_titles("{col_name}")
g4.fig.suptitle('Impact of file_buffer_size on Execution Times', y=1.03)
plt.show()

#%%

subset3 = plot_df[
    (plot_df['buffer_size'] == 4096) &
    (plot_df['file_buffer_size'] == 0) &
    (plot_df['keys'] == "f0_f1")
].copy()

# Group by separation method.
grouped_sep = subset3.groupby('sep_method').agg({
    'writing_time': 'mean',
    'truncating_time': 'mean',
    'reading_time': 'mean'
}).reset_index()

# Melt the DataFrame for a grouped (side-by-side) bar plot.
melted3 = pd.melt(grouped_sep,
                  id_vars='sep_method',
                  value_vars=['writing_time', 'truncating_time', 'reading_time'],
                  var_name='phase',
                  value_name='time')

plt.figure(figsize=(10,6))
sns.barplot(data=melted3, x='sep_method', y='time', hue='phase')
plt.title('Execution Times by Separation Method\n(buffer_size = 4096, file_buffer = 0, keys = f0_f1)')
plt.ylabel('Execution Time')
plt.xlabel('Separation Method')
plt.tight_layout()
plt.show()

#%%

subset4 = plot_df[
    (plot_df['buffer_size'] == 4096) &
    (plot_df['file_buffer_size'] == 0) &
    (plot_df['keys'].isin(selected_keys))
].copy()

# Group by separation method and keys.
grouped_sep_keys = subset4.groupby(['sep_method', 'keys']).agg({
    'writing_time': 'mean',
    'truncating_time': 'mean',
    'reading_time': 'mean'
}).reset_index()

# Melt the data for a grouped bar plot.
melted4 = pd.melt(grouped_sep_keys,
                  id_vars=['sep_method', 'keys'],
                  value_vars=['writing_time', 'truncating_time', 'reading_time'],
                  var_name='phase',
                  value_name='time')

# Use a facet grid to show each phase separately.
g4 = sns.catplot(
    data=melted4, x='sep_method', y='time', hue='keys', col='phase',
    kind='bar', height=4, aspect=1.2, ci="sd"
)
g4.fig.suptitle('Execution Times by Separation Method & Keys\n(buffer_size = 4096, file_buffer = 0)', y=1.05)
plt.show()

#%%

subset5 = plot_df[
    (plot_df['buffer_size'] == 4096) &
    (plot_df['keys'].isin(selected_keys))
].copy()

# Melt for plotting.
melted5 = pd.melt(subset5,
                  id_vars=['file_buffer_size', 'sep_method', 'keys'],
                  value_vars=['writing_time', 'truncating_time', 'reading_time'],
                  var_name='phase',
                  value_name='time')

# Create a line plot with separate facets for each phase.
g5 = sns.relplot(
    data=melted5,
    x='file_buffer_size', y='time', hue='sep_method', style='keys',
    col='phase', kind='line', marker='o',
    facet_kws={'sharey': False, 'sharex': True},
    height=4, aspect=1.2, estimator=np.mean
)
g5.set_titles("{col_name}")
g5.fig.suptitle('Impact of file_buffer (buffer_size = 4096) on Execution Times', y=1.03)
plt.show()

# for group_key, group_data in combined_df.groupby(group_columns):
#     # For the corresponding cleaned group, filter clean_df using the same configuration.
#     subset_filtered = filtered_outliers_df.copy()
#     for col, val in zip(group_columns, group_key):
#         subset_filtered = subset_filtered[subset_filtered[col] == val]

#     # Create a figure with two subplots: left = original, right = cleaned.
#     fig, axes = plt.subplots(1, 2, figsize=(12, 6))
#     fig.suptitle(f"Configuration: {group_key}", fontsize=14)

#     # Plot boxplots for the original group data.
#     sns.boxplot(data=group_data[columns_to_clean], ax=axes[0])
#     axes[0].set_title("Before Outlier Removal")
#     axes[0].set_ylabel("Execution Time")
#     axes[0].set_xlabel("Timing Metric")

#     # Plot boxplots for the cleaned group data.
#     sns.boxplot(data=subset_filtered[columns_to_clean], ax=axes[1])
#     axes[1].set_title("After Outlier Removal")
#     axes[1].set_ylabel("Execution Time")
#     axes[1].set_xlabel("Timing Metric")

#     plt.tight_layout(rect=[0, 0, 1, 0.95])  # Adjust layout so the suptitle is visible.
#     plt.show()
