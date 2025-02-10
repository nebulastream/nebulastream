#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 13:07:09 2025

@author: nikla
"""

import os
import re
import glob
import math
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Change this to the directory where your CSV files are located
data_dir = "data/03-02-4GB"
file_pattern = os.path.join(data_dir, "micro_benchmarks_BulkWriteTest_*.csv")
files = glob.glob(file_pattern)

# Regular expression to parse filenames
# micro_benchmarks_BulkWriteTest_NO_SEPARATION_bufferSize_<buffer_size>_fileBufferSizePercent_<data_size>.csv
pattern = re.compile(
    r"micro_benchmarks_BulkWriteTest_NO_SEPARATION_bufferSize_(?P<buffer_size>\d+)_fileBufferSizePercent_(?P<data_size>\d+)\.csv"
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

    # Extract parameters from filename and convert to int
    buffer_size = int(match.group("buffer_size"))
    data_size = int(match.group("data_size"))

    # Read CSV file
    df = pd.read_csv(file, header=None)

    # Verify that we have the expected 8 columns
    if df.shape[1] != 8:
        print(f"File {basename} does not have 8 columns. Skipping.")
        continue

    df.columns = [f'col{i}' for i in range(1, 9)]

    # Rename columns to clarify their roles
    df = df.rename(columns={
        'col1': 'generate_data',
        'col2': 'write_exec_time',
        'col3': 'def_var',
        'col4': 'alloc_mem',
        'col5': 'open_file',
        'col6': 'write_buf',
        'col7': 'write_file',
        'col8': 'close_file',
        'col9': 'compare_data'
    })

    # Calculate time to write to file without memory allocation
    exec_time = df['write_exec_time'] - df['alloc_mem']
    df['execution_time'] = exec_time / 1000

    # Attach Metadata to each row
    df["buffer_size"] = int(buffer_size / 1024)
    df["data_size"] = data_size

    # Append the processed DataFrame
    data_frames.append(df)

# Combine all experiments from all files into one DataFrame
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
else:
    raise RuntimeError("No valid CSV files found!")

# Calculate the percentage of execution time for defining variables
combined_df['def_var_percent'] = ((combined_df['def_var'] / 1000) / combined_df['execution_time']) * 100

# Sort the combined values
combined_df = combined_df.sort_values(["buffer_size", "data_size"])

# Ensure buffer_size is treated as a string for discrete grouping
combined_df['buffer_size'] = combined_df['buffer_size'].astype(str)

#%% Calculate outliers

outliers = []
multiplier = 1.5

# Group by (buffer_size, data_size)
for (buf, size), group in combined_df.groupby(["buffer_size", "data_size"]):
    Q1 = group["execution_time"].quantile(0.25)
    Q3 = group["execution_time"].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR

    # Find outliers in this group
    outlier_rows = group[(group["execution_time"] < lower_bound) | (group["execution_time"] > upper_bound)]
    
    if not outlier_rows.empty:
        outliers.append(outlier_rows)

# Combine all outliers found
outliers_df = pd.concat(outliers) if outliers else pd.DataFrame()
print(f"Total outliers found: {len(outliers_df)}")

filtered_outliers_df = combined_df[~combined_df.index.isin(outliers_df.index)]

#%% Define functions

def create_simple_lineplot(data):
    # Create a line plot using seaborn
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=data,
        x='data_size',
        y='execution_time',
        hue='buffer_size',
        estimator='mean',
        errorbar='sd',
        marker='o'
    )

    plt.title("Execution Time vs Data Size for Different Buffer Sizes")
    plt.xlabel("Data Size (B)")
    plt.ylabel("Average Execution Time (sec)")
    plt.legend(title="Buffer Size (kB)")
    plt.tight_layout()
    plt.show()

def create_splitted_catplots(data, data_splits):
    # Generate multiple plots
    for i, buffer_subset in enumerate(data_splits):
        num_buffers = len(buffer_subset)

        # Adjust col_wrap to ensure 2x2
        col_wrap = min(2, num_buffers)

        g = sns.catplot(
            data=data[data["buffer_size"].isin(buffer_subset)],
            x="data_size",
            y="execution_time",
            col="buffer_size",
            kind="box",
            col_wrap=col_wrap,
            sharey=False
        )

        # Rotate and format for readability
        for ax in g.axes.flat:
            ax.tick_params(axis='x', rotation=45)
            #ax.set_xticklabels([f'{tick:.2f}' for tick in ax.get_xticks()])

        g.fig.suptitle(f"Boxplot of Execution Times (Subset {i + 1})", y=1.02)
        plt.show()

#%% Show all data

create_simple_lineplot(combined_df)

#%% Show outliers

# Define number of buffer sizes per plot
buffers_per_plot = math.ceil(len(combined_df["buffer_size"].unique()) / 2)

# Get buffer sizes and create subsets
buffer_sizes = combined_df["buffer_size"].unique()
buffer_splits = [buffer_sizes[i:i + buffers_per_plot] for i in range(0, len(buffer_sizes), buffers_per_plot)]

create_splitted_catplots(combined_df, buffer_splits)

#%% Show all data with outliers removed

create_simple_lineplot(filtered_outliers_df)

# Define number of buffer sizes per plot
buffers_per_plot = math.ceil(len(filtered_outliers_df["buffer_size"].unique()) / 2)

# Get buffer sizes and create subsets
buffer_sizes = filtered_outliers_df["buffer_size"].unique()
buffer_splits = [buffer_sizes[i:i + buffers_per_plot] for i in range(0, len(buffer_sizes), buffers_per_plot)]

create_splitted_catplots(filtered_outliers_df, buffer_splits)

#%% Show data with data_size < 1MB and < 32MB and outliers removed

# Define the data_size threshold
upper_bound = 1024 * 1024
lower_bound = 0

# Filter the DataFrame to only include rows with data_size greater than the threshold
filtered_datasizes_df = filtered_outliers_df[(filtered_outliers_df['data_size'] >= lower_bound) & (filtered_outliers_df['data_size'] <= upper_bound)]

create_simple_lineplot(filtered_datasizes_df)

# Define the data_size threshold
upper_bound = 32 * 1024 * 1024
lower_bound = 1024 * 1024

# Filter the DataFrame to only include rows with data_size greater than the threshold
filtered_datasizes_df = filtered_outliers_df[(filtered_outliers_df['data_size'] >= lower_bound) & (filtered_outliers_df['data_size'] <= upper_bound)]

create_simple_lineplot(filtered_datasizes_df)

#%% Show share of defining variables compared to total execution time with data_size < 1MB and outliers removed

# Define the data_size threshold
upper_bound = 512 * 1024
lower_bound = 0

# Filter the DataFrame to only include rows with data_size greater than the threshold
filtered_datasizes_df = filtered_outliers_df[(filtered_outliers_df['data_size'] >= lower_bound) & (filtered_outliers_df['data_size'] <= upper_bound)]

# Create a lineplot to visualize this percentage
plt.figure(figsize=(10, 6))

sns.lineplot(
    data=filtered_datasizes_df,
    x='data_size',
    y='def_var_percent',
    hue='buffer_size',
    marker='o'
)

plt.title("Percentage of Execution Time for Defining Variables by Buffer Size and Data Size")
plt.xlabel("Data Size (B)", fontsize=12)
plt.ylabel("Execution Time (%)", fontsize=12)
plt.legend(title="Buffer Size (kB)")
plt.tight_layout()
plt.show()
