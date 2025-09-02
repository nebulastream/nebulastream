#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import argparse
from pathlib import Path

def generate_data(num_rows=10000000, num_columns=10, file_path="benchmark_data.csv"):
    """Generate test data with configurable columns and distribution for different selectivities"""
    # Check if file already exists
    if os.path.exists(file_path):
        file_size_gb = os.path.getsize(file_path) / (1024*1024*1024)
        print(f"Using existing data file: {file_path}")
        print(f"File size: {file_size_gb:.2f} GB")

        # Read the first row to get column list
        sample_df = pd.read_csv(file_path, nrows=1)
        return file_path, sample_df.columns.tolist()

    # Calculate rows needed for ~1.5GB file
    bytes_per_row = num_columns * 10  # ~8 bytes per UINT64 + CSV overhead
    target_size = 1.5 * 1024 * 1024 * 1024  # Target 1.5GB
    num_rows = min(num_rows, int(target_size / bytes_per_row))

    # Create data dictionary
    data = {}

    # Generate columns with varied distributions for filter testing
    for i in range(num_columns):
        column_name = f"col_{i}"

        if i % 3 == 0:
            # Uniform distribution - good for predictable selectivity
            data[column_name] = np.random.randint(0, 2**32-1, size=num_rows, dtype=np.uint64)
        elif i % 3 == 1:
            # Normal distribution (clipped to positive values)
            values = np.random.normal(2**31, 2**30, size=num_rows)
            data[column_name] = np.clip(values, 0, 2**32-1).astype(np.uint64)
        else:
            # Exponential distribution - good for skewed data
            values = np.random.exponential(2**28, size=num_rows)
            data[column_name] = values.astype(np.uint64)

    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    df.to_csv(file_path, index=False, header=False)

    file_size_gb = os.path.getsize(file_path) / (1024*1024*1024)
    print(f"Generated {num_rows} rows with {num_columns} columns at {file_path}")
    print(f"File size: {file_size_gb:.2f} GB")

    return file_path, df.columns.tolist()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark data')
    parser.add_argument('--rows', type=int, default=10000000, help='Maximum number of rows')
    parser.add_argument('--columns', type=int, default=10, help='Number of columns')
    parser.add_argument('--output', type=str, default='benchmark_data.csv', help='Output file path')
    args = parser.parse_args()

    # Create output directories if they don't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True, parents=True)

    generate_data(args.rows, args.columns, args.output)