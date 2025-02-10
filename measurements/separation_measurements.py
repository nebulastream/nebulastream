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
columns = ["writing_time", "truncate_file", "reading_time"]

mask = pd.Series(True, index=combined_df.index)
for col in columns:
    Q1 = combined_df[col].quantile(0.25)
    Q3 = combined_df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    # Create a mask for rows where the value is within the acceptable range
    mask &= combined_df[col].between(lower_bound, upper_bound)

filtered_outliers_df = combined_df[mask]

#%%

# ----------------------------
# 5. Visualization Using Seaborn
# ----------------------------
plot_df = combined_df
sns.set(style="whitegrid")

# Example 1: Bar plots comparing execution times by separation method (with/without keys)
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
sns.barplot(data=plot_df, x="sep_method", y="writing_time", hue="keys", ax=axes[0], estimator=np.mean)
axes[0].set_title("Writing Time by Separation Method")
sns.barplot(data=plot_df, x="sep_method", y="truncate_file", hue="keys", ax=axes[1], estimator=np.mean)
axes[1].set_title("Truncating Time by Separation Method")
sns.barplot(data=plot_df, x="sep_method", y="reading_time", hue="keys", ax=axes[2], estimator=np.mean)
axes[2].set_title("Reading Time by Separation Method")
plt.tight_layout()
plt.show()

# Example 2: Line plot showing the impact of buffer size on writing time.
g = sns.relplot(
    data=plot_df, x="buffer_size", y="writing_time", hue="sep_method",
    col="keys", kind="line", marker="o", facet_kws={'sharey': False, 'sharex': True},
    height=4, aspect=1.2, estimator=np.mean
)
g.set_titles("Keys: {col_name}")
g.fig.suptitle("Writing Time vs. Buffer Size", y=1.05)
plt.show()

# Example 3: Scatter plot showing the impact of file-buffer percentage on reading time.
g2 = sns.relplot(
    data=plot_df, x="file_buffer_size", y="reading_time", hue="sep_method",
    style="keys", kind="scatter", height=5, aspect=1.2
)
g2.fig.suptitle("Reading Time vs. File Buffer Percentage", y=0.95)
plt.show()

# Assuming plot_df is your combined DataFrame
# Plot histograms for each timing column
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

sns.histplot(plot_df["writing_time"], kde=True, ax=axes[0])
axes[0].set_title("Distribution of Writing Time")

sns.histplot(plot_df["truncate_file"], kde=True, ax=axes[1])
axes[1].set_title("Distribution of Truncating Time")

sns.histplot(plot_df["reading_time"], kde=True, ax=axes[2])
axes[2].set_title("Distribution of Reading Time")

plt.tight_layout()
plt.show()

# Additionally, boxplots can quickly highlight outliers
sns.boxplot(data=plot_df[["writing_time", "truncate_file", "reading_time"]])
plt.title("Boxplots of Execution Times")
plt.show()
