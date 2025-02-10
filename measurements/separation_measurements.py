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

# Tolerance for the validation of the relation between columns
tolerance = 0.1
# Container to hold data from all files
data_frames = []

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

    # Read CSV without header
    df = pd.read_csv(file, header=None)

    # Verify that we have the expected 17 columns
    if df.shape[1] != 17:
        print(f"File {basename} does not have 17 columns. Skipping.")
        continue

    # Calculate time to write to file without memory allocation
    writing_time_option1 = df[1] - df[3]
    writing_time_option2 = df[2] + df[4] + df[5] + df[6] + df[7]
    writing_error = np.abs(writing_time_option1 - writing_time_option2)
    invalid_rows = writing_error > tolerance
    if invalid_rows.any():
        num_invalid = invalid_rows.sum()
        print(f"Warning: Writing phase sanity check failed for {num_invalid} rows for {basename}")

    df["writing_time"] = writing_time_option2

    # Add time to truncate data
    df["truncating_time"] = df[8]

    # Calculate time to read from file without memory allocation
    reading_time_option1 = df[9] - df[11]
    reading_time_option2 = df[10] + df[12] + df[13] + df[14] + df[15]
    reading_error = np.abs(reading_time_option1 - reading_time_option2) / reading_time_option1
    invalid_rows = reading_error > tolerance
    if invalid_rows.any():
        num_invalid = invalid_rows.sum()
        print(f"Warning: Reading phase sanity check failed for {num_invalid} rows for {basename}")

    df["reading_time"] = reading_time_option2

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

# Sort the combined values
#combined_df = combined_df.sort_values(by="keys", key=lambda x: x.str.len())
combined_df = combined_df.sort_values(["sep_method", "buffer_size", "file_buffer_size"])

# Select only the columns of interest for plotting
plot_df = combined_df[["sep_method", "buffer_size", "file_buffer_size", "keys",
                   "writing_time", "truncating_time", "reading_time"]]

# ----------------------------
# 5. Visualization Using Seaborn
# ----------------------------
sns.set(style="whitegrid")

# Example 1: Bar plots comparing execution times by separation method (with/without keys)
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
sns.barplot(data=plot_df, x="sep_method", y="writing_time", hue="keys", ax=axes[0], estimator=np.mean)
axes[0].set_title("Writing Time by Separation Method")
sns.barplot(data=plot_df, x="sep_method", y="truncating_time", hue="keys", ax=axes[1], estimator=np.mean)
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
