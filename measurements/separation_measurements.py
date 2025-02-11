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

    # Attach Metadata to each row
    df["sep_method"] = sep_method
    df["buffer_size"] = int(buffer_size / 1024)
    df["file_buffer_size"] = file_buffer_size
    df["keys"] = keys

    # Append the processed DataFrame
    data_frames.append(df)

# Combine all experiments from all files into one DataFrame
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
else:
    raise RuntimeError("No valid CSV files found!")

# Calculate the percentage of execution time for defining variables
combined_df['def_var_write_percent'] = ((combined_df['def_var_write'] / 1000) / combined_df['write_exec_time']) * 100
combined_df['def_var_read_percent'] = ((combined_df['def_var_read'] / 1000) / combined_df['read_exec_time']) * 100

# Sort the combined values
#combined_df = combined_df.sort_values(by="keys", key=lambda x: x.str.len())
combined_df = combined_df.sort_values(["sep_method", "buffer_size", "file_buffer_size"])

# Ensure buffer_size is treated as a string for discrete grouping
#combined_df['buffer_size'] = combined_df['buffer_size'].astype(str)

#%% Remove outliers

multiplier = 1.5
filtered_outliers_groups = []
columns_to_clean = ["writing_time", "truncate_file", "reading_time"]
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

#%%

# --- Step 1: Select Representative Buffer Sizes and File Buffer Sizes ---
plot_df = filtered_outliers_df

# Choose three representative values for each
selected_buffer_sizes = [1, 4, 16]
selected_file_buffer_sizes = [0, 60, 100]
selected_keys = ["no_keys", "f0_f1", "f0_f1_f2_f3_f4", "f0_f5", "f0_f2_f4_f6_f8"]

# Filter the DataFrame for only these values:
subset_plot_df = plot_df[
    (plot_df['buffer_size'].isin(selected_buffer_sizes)) &
    (plot_df['file_buffer_size'].isin(selected_file_buffer_sizes) &
    (plot_df['keys'].isin(selected_keys)))
].copy()

# --- Step 2: Aggregate by Separation Method ---
# For the overall view, let’s group by separation method (ignoring keys for now)
# and compute the mean execution times of the phases.
grouped = subset_plot_df.groupby("sep_method").agg({
    "writing_time": "mean",
    "truncate_file": "mean",
    "reading_time": "mean"
}).reset_index()

# --- Step 3: Create a Stacked Barplot using matplotlib ---

# We want a stacked bar where:
# - The bottom segment is writing_time,
# - Then truncating_time is stacked on top,
# - Then reading_time is on top.
labels = grouped['sep_method']
writing = grouped['writing_time']
truncating = grouped['truncate_file']
reading = grouped['reading_time']

x = np.arange(len(labels))  # label locations
width = 0.6  # width of the bars

fig, ax = plt.subplots(figsize=(10, 6))

# Plot the three segments of the stacked bar
bar1 = ax.bar(x, writing, width, label='Writing Time')
bar2 = ax.bar(x, truncating, width, bottom=writing, label='Truncating Time')
bar3 = ax.bar(x, reading, width, bottom=writing+truncating, label='Reading Time')

ax.set_ylabel('Execution Time')
ax.set_title('Average Execution Times (Stacked) by Separation Method\n(for 3 representative buffer sizes and file buffer sizes)')
ax.set_xticks(x)
ax.set_xticklabels(labels, rotation=45)
ax.legend()

plt.tight_layout()
plt.show()

#%%

# Create a new column for the accumulated execution time:
plot_df['total_time'] = plot_df['writing_time'] + plot_df['truncate_file'] + plot_df['reading_time']

# Plot writing time vs. buffer_size with separation method as hue, and facet by keys.
sns.relplot(
    data=plot_df, 
    x="buffer_size", y="writing_time", 
    hue="sep_method", 
    col="keys", 
    kind="line", 
    marker="o", 
    facet_kws={'sharey': False, 'sharex': True},
    height=4, aspect=1.2, 
    estimator=np.mean  # using mean values; you could also try median
)
plt.suptitle("Writing Time vs. Buffer Size", y=1.05)
plt.show()

# Similarly, you could plot total_time or reading_time vs. buffer_size.

#%%

sns.relplot(
    data=plot_df, 
    x="file_buffer_size", y="total_time", 
    hue="sep_method", 
    style="keys", 
    kind="scatter", 
    height=5, aspect=1.2
)
plt.title("Total Execution Time vs. File Buffer Size")
plt.show()

#%%

plt.figure(figsize=(10,6))
sns.boxplot(data=plot_df, x="sep_method", y="total_time", hue="keys")
plt.title("Total Execution Time by Separation Method\n(comparing configurations with/without keys)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#%%

# Melt the DataFrame so that timing columns are in one column
melted = pd.melt(plot_df, 
                 id_vars=["sep_method", "buffer_size", "file_buffer_size", "keys"],
                 value_vars=["writing_time", "truncate_file", "reading_time"],
                 var_name="phase", value_name="time")

# Plot a barplot showing the average time per phase for each separation method:
sns.catplot(
    data=melted, 
    x="sep_method", y="time", hue="phase", 
    kind="bar", height=6, aspect=1.2,
    ci="sd"
)
plt.title("Average Execution Time per Phase by Separation Method")
plt.xticks(rotation=45)
plt.tight_layout()
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
