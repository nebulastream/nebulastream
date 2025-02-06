#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 13:07:09 2025

@author: nikla
"""

import re
import glob
import math
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

# Pattern for filename matching and extraction
filename_pattern = re.compile(
    r"micro_benchmarks_BulkWriteTest_NO_SEPARATION_bufferSize_(?P<bufferSize>\d+)_fileBufferSizePercent_(?P<dataSize>\d+)\.csv"
)

# List all CSV files matching the pattern
file_list = glob.glob("data/03-02-4GB/micro_benchmarks_BulkWriteTest_NO_SEPARATION_bufferSize_*_fileBufferSizePercent_*.csv")

# Tolerance for the validation of the relation between columns
tolerance = 1e-2

# Container to hold data from all files
all_data = []

# Process each file
for file in file_list:
    match = filename_pattern.search(file)
    if not match:
        print(f"Filename {file} does not match the expected pattern. Skipping.")
        continue

    # Extract parameters from filename and convert to int
    buffer_size = int(match.group("bufferSize"))
    data_size = int(match.group("dataSize"))

    # Read CSV file assuming each CSV has exactly 8 columns
    df = pd.read_csv(file, header=None)
    df.columns = [f'col{i}' for i in range(1, 9)]

    # Disregard the first column
    df = df.drop(columns=['col1'])

    # Rename columns to clarify their roles
    df = df.rename(columns={
        'col2': 'overall_time',
        'col3': 'def_var',
        'col4': 'alloc_mem',
        'col5': 'open_file',
        'col6': 'write_buf',
        'col7': 'write_file',
        'col8': 'close_file'
    })

    # Compute the execution time in two ways
    exec_time_option1 = df['overall_time'] - df['alloc_mem']
    exec_time_option2 = df['def_var'] + df['open_file'] + df['write_buf'] + df['write_file'] + df['close_file']

    # Validate that the two computations are almost equal
    diff = abs(exec_time_option1 - exec_time_option2)
    invalid_rows = diff > tolerance
    if invalid_rows.any():
        num_invalid = invalid_rows.sum()
        print(f"Warning: {num_invalid} rows in file {file} do not satisfy the execution time equality within the tolerance of {tolerance}.")

    # For plotting, choose one option
    df['execution_time'] = exec_time_option2 / 1000

    # Add columns for the parameters extracted from the filename
    df['buffer_size'] = int(buffer_size / 1024)
    df['data_size'] = data_size

    # Append the processed DataFrame to our list
    all_data.append(df)

# Combine data from all files into a single DataFrame
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
else:
    raise ValueError("No data was loaded. Check your file path or filename pattern.")

# Calculate the percentage of execution time for defining variables
combined_df['execution_time_def_var_percentage'] = (combined_df['def_var'] / (1000 * combined_df['execution_time'])) * 100

# Sort the combined values
combined_df = combined_df.sort_values(by="buffer_size")

# Ensure buffer_size is treated as a string for discrete grouping
combined_df['buffer_size'] = combined_df['buffer_size'].astype(str)

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
outliers = []

# Group by (buffer_size, data_size)
for (buf, size), group in combined_df.groupby(["buffer_size", "data_size"]):
    Q1 = group["execution_time"].quantile(0.25)
    Q3 = group["execution_time"].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Find outliers in this group
    outlier_rows = group[(group["execution_time"] < lower_bound) | (group["execution_time"] > upper_bound)]
    
    if not outlier_rows.empty:
        outliers.append(outlier_rows)

# Combine all outliers found
outliers_df = pd.concat(outliers) if outliers else pd.DataFrame()
print(f"Total outliers found: {len(outliers_df)}")
#print(outliers_df[["buffer_size", "data_size", "execution_time"]])

filtered_outliers_df = combined_df[~combined_df.index.isin(outliers_df.index)]

create_simple_lineplot(filtered_outliers_df)

# Define number of buffer sizes per plot
buffers_per_plot = math.ceil(len(filtered_outliers_df["buffer_size"].unique()) / 2)

# Get buffer sizes and create subsets
buffer_sizes = filtered_outliers_df["buffer_size"].unique()
buffer_splits = [buffer_sizes[i:i + buffers_per_plot] for i in range(0, len(buffer_sizes), buffers_per_plot)]

create_splitted_catplots(filtered_outliers_df, buffer_splits)

#%% Show data with data_size < 1MB and outliers removed

# Define the data_size threshold
upper_bound = 1024 * 1024
lower_bound = 0

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
    y='execution_time_def_var_percentage',
    hue='buffer_size',
    marker='o'
)

plt.title("Percentage of Execution Time for Defining Variables by Buffer Size and Data Size")
plt.xlabel("Data Size (B)", fontsize=12)
plt.ylabel("Execution Time (%)", fontsize=12)
plt.legend(title="Buffer Size (kB)")
plt.tight_layout()
plt.show()
