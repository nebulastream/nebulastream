#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 08:24:51 2025

@author: ntantow
"""

import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_measurements():
    # Glob for all csv files in the subdirectory
    data_dir = "data"
    pattern = os.path.join(data_dir, "micro_benchmarks_*.csv")
    files = glob.glob(pattern)

    if not files:
        print("No files found matching the pattern:", pattern)
        return

    # Regular expression to parse the file names
    regex = re.compile(
        r"micro_benchmarks_(?P<method>.+)_bufferSize_(?P<buffer_size>\d+)_fileBufferSizePercent_(?P<file_buffer_size>\d+)_keys(?:_(?P<keys>.+))?\.csv"
    )

    # Dictionary to accumulate averages and counts
    data = {}

    for file in files:
        base = os.path.basename(file)
        m = regex.fullmatch(base)
        if not m:
            print("Skipping file (unexpected format):", file)
            continue

        # Extract parts from the file name
        method = m.group("method")
        buffer_size_str = m.group("buffer_size")
        file_buffer_size_str = m.group("file_buffer_size")
        keys_str = m.group("keys") or ""

        # Filter out files
        if method == "SAME_FILE_KEYS" or method == "SAME_FILE_PAYLOAD" or buffer_size_str == "262144" or file_buffer_size_str != "0" or keys_str == "":
            continue

        # Create a key representing the combination
        key = (method, buffer_size_str, file_buffer_size_str, keys_str)

        try:
            # Read the CSV file
            df = pd.read_csv(file, header=None)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

        # Check that the CSV has at least 5 rows and 5 columns
        if df.shape[0] < 5 or df.shape[1] < 5:
            print("Skipping file due to unexpected shape:", file)
            continue

        # Compute average times over the 5 experiments (rows)
        avg_write = df.iloc[:, 1].mean()
        avg_truncate = df.iloc[:, 2].mean()
        avg_read = df.iloc[:, 3].mean()

        if key not in data:
            data[key] = [0.0, 0.0, 0.0, 0]
        else:
            print("Multiple files for {}_{}_{}_{} found.".format(method, buffer_size_str, file_buffer_size_str, keys_str))
            return

        data[key][0] += avg_write
        data[key][1] += avg_truncate
        data[key][2] += avg_read
        data[key][3] += 1

    if not data:
        print("No files were processed after parsing.")
        return

    # Sort the keys for consistent ordering
    sorted_keys = sorted(
        data.keys(),
        key=lambda k: (k[0], int(k[1]), int(k[2]), k[3])
    )

    # Create lists for plotting
    tick_labels = []
    write_times = []
    truncate_times = []
    read_times = []
    methods_list = []
    sizes_list = []

    for key in sorted_keys:
        method, buffer_size_str, file_buffer_size_str, keys_str = key
        total_write, total_truncate, total_read, count = data[key]
        methods_list.append(method)
        sizes_list.append(buffer_size_str)
        # Use the tick label to display only fileBufferSizePercent and keys
        label = f"{file_buffer_size_str}" + (f", {keys_str}" if keys_str else "-")
        tick_labels.append(label)
        write_times.append((total_write / count) / 1000)
        truncate_times.append((total_truncate / count) / 1000)
        read_times.append((total_read / count) / 1000)

    # Create a stacked bar chart
    fig, ax = plt.subplots(figsize=(16, 8))
    x = np.arange(len(sorted_keys))

    # Plot the "write" times
    ax.bar(x, write_times, label="Write")
    # Plot the "truncate" times on top of the "write" times
    ax.bar(x, truncate_times, bottom=write_times, label="Truncate")
    # Compute the bottom for the "read" times
    bottom_for_read = [w + t for w, t in zip(write_times, truncate_times)]
    ax.bar(x, read_times, bottom=bottom_for_read, label="Read")

    # Set the tick labels for each bar
    ax.set_xticks(x)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right")
    ax.set_xlabel("fileBufferSize [% of BufferSize], Keys")
    ax.set_ylabel("Execution Time [sec]")
    ax.set_title("Stacked Execution Times by Benchmark Parameters")
    ax.legend()

    # Group positions for methods
    group_positions_method = {}
    for i, method in enumerate(methods_list):
        group_positions_method.setdefault(method, []).append(i)

    # Group positions for bufferSize (grouped within each method)
    group_positions_size = {}
    for i, (method, size) in enumerate(zip(methods_list, sizes_list)):
        group_positions_size.setdefault((method, size), []).append(i)

    # Draw the bufferSize group labels (just below tick labels)
    for (method, size), indices in group_positions_size.items():
        center = np.mean(indices)
        ax.text(center, -0.35, f"{size}", ha="center", va="top",
                transform=ax.get_xaxis_transform(), fontsize=11, fontstyle="italic")

    ax.text(-0.5, -0.35, "BufferSize [B]", ha="right", va="top",
            transform=ax.get_xaxis_transform(), fontsize=11)

    # Draw the method group labels (at the very bottom)
    for method, indices in group_positions_method.items():
        center = np.mean(indices)
        ax.text(center, -0.40, method, ha="center", va="top",
                transform=ax.get_xaxis_transform(), fontsize=12, fontweight="bold")

    # Increase the bottom margin to ensure that both labels are visible
    plt.subplots_adjust(bottom=0.40)
    plt.show()

# To run the function:
if __name__ == '__main__':
    plot_measurements()
