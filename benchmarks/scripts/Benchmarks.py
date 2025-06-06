#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 09:04:05 2025

@author: ntantow
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Optional: widen display
pd.set_option("display.max_columns", None)

# Load the CSV
df = pd.read_csv("../data/amd/2025-06-05_10-15-06/combined_benchmark_statistics.csv")

# Drop prototype-only columns since they contain junk data for Default
prototype_only_cols = [
    "num_watermark_gaps_allowed", "max_num_sequence_numbers", "file_descriptor_buffer_size",
    "min_read_state_size", "min_write_state_size", "file_operation_time_delta",
    "file_layout", "watermark_predictor_type"
]
df = df.drop(columns=prototype_only_cols, errors="ignore")

# Normalize time within each experiment
def normalize_time(group):
    min_time = group["window_start_normalized"].min()
    max_time = group["window_start_normalized"].max()
    group["time_normalized"] = (group["window_start_normalized"] - min_time) / (max_time - min_time)
    return group

# Define shared config columns
shared_config_cols = [
    "buffer_size_in_bytes", "buffers_in_global_buffer_manager", "buffers_in_source_local_buffer_pool",
    "buffers_per_worker", "execution_mode", "ingestion_rate", "number_of_worker_threads",
    "page_size", "query", "task_queue_size", "timestamp_increment"
]

# Apply normalization with index reset to avoid ambiguity
df = df.groupby(shared_config_cols + ["slice_store_type"]).apply(normalize_time).reset_index(drop=True)

# --- Step 1: Compare Baseline vs Prototype for a Sample Config ---

# Find configs available for both slice store types
config_counts = df.groupby(shared_config_cols)["slice_store_type"].nunique()
comparable_configs = config_counts[config_counts == 2].reset_index()

# Pick the first comparable config
sample_config = comparable_configs.iloc[0].to_dict()

# Filter for both slice store types that match the config
mask = pd.Series(True, index=df.index)
for col, val in sample_config.items():
    mask &= df[col] == val
filtered = df[mask]

# Plot throughput and memory over normalized time
for metric in ["throughput_data", "memory"]:
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=filtered, x="time_normalized", y=metric, hue="slice_store_type")
    plt.title(f"{metric.replace('_', ' ').title()} Over Time (Normalized) Config: {sample_config}")
    plt.xlabel("Normalized Time")
    plt.ylabel(metric.replace("_", " ").title())
    plt.tight_layout()
    plt.show()

# --- Step 2: Per-Parameter Impact on Throughput and Memory ---

# Compute average throughput/memory per experiment
grouped = df.groupby(shared_config_cols + ["slice_store_type"]).agg({
    "throughput_data": "mean",
    "memory": "mean"
}).reset_index()

# Plot for each shared parameter
for param in shared_config_cols:
    if grouped[param].nunique() < 2:
        continue

    for metric in ["throughput_data", "memory"]:
        plt.figure(figsize=(10, 5))
        sns.lineplot(data=grouped, x=param, y=metric, hue="slice_store_type", marker="o")
        plt.title(f"Avg. {metric.replace('_', ' ').title()} vs {param}")
        plt.xlabel(param)
        plt.ylabel(f"Avg. {metric.replace('_', ' ').title()}")
        plt.tight_layout()
        plt.show()
