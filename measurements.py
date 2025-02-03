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

def plot_measurements():
    # Glob for all csv files in the subdirectory
    data_dir = "measurements"
    pattern = os.path.join(data_dir, "micro_benchmarks_*.csv")
    files = glob.glob(pattern)

    if not files:
        print("No files found matching the pattern:", pattern)
        return

    # Regular expression to parse the file names
    regex = re.compile(
        r"micro_benchmarks_(?P<method>.+)_bufferSize_(?P<buffer_size>\d+)_fileBufferSizePercent_(?P<file_buffer_size>\d+)_keys(?:_(?P<keys>.+))?\.csv"
    )

    # Dictionaries to accumulate averages and counts for each method
    averages = {} # method -> [sum_write, sum_truncate, sum_read]
    counts = {}   # method -> number of files

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
        if buffer_size_str != "4096" or file_buffer_size_str != "0" or keys_str != "f0_f1":
            continue

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

        if method not in averages:
            averages[method] = [0.0, 0.0, 0.0]
            counts[method] = 0
        averages[method][0] += avg_write
        averages[method][1] += avg_truncate
        averages[method][2] += avg_read
        counts[method] += 1

    # If no matching files were processed, exit.
    if not averages:
        print("No files met the filtering criteria.")
        return

    # Prepare lists for plotting.
    methods = []
    write_times = []
    truncate_times = []
    read_times = []
    for method, sums in averages.items():
        n = counts[method]
        methods.append(method)
        write_times.append(sums[0] / n)
        truncate_times.append(sums[1] / n)
        read_times.append(sums[2] / n)

    # Sort the methods alphabetically for a consistent order
    methods, write_times, truncate_times, read_times = zip(*sorted(zip(methods, write_times, truncate_times, read_times)))

    # Create a stacked bar chart
    fig, ax = plt.subplots(figsize=(8, 6))
    x = range(len(methods))

    # Plot the "write" times
    bar1 = ax.bar(x, write_times, label="Write")
    # Plot the "truncate" times on top of the "write" times
    bar2 = ax.bar(x, truncate_times, bottom=write_times, label="Truncate")
    # Compute the bottom for the "read" times
    bottom_for_read = [w + t for w, t in zip(write_times, truncate_times)]
    bar3 = ax.bar(x, read_times, bottom=bottom_for_read, label="Read")

    # Label the x-axis with the method names
    ax.set_xticks(x)
    ax.set_xticklabels(methods, rotation=45, ha="right")
    ax.set_xlabel("Method")
    ax.set_ylabel("Execution Time")
    ax.set_title("Stacked Execution Times (Write, Truncate, Read)")
    ax.legend()

    plt.tight_layout()
    plt.show()

# To run the function:
if __name__ == '__main__':
    plot_measurements()
