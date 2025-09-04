#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import argparse
import json
from pathlib import Path
from benchmark_system import parse_int_list

def generate_data(num_rows=10000000, num_columns=10, file_path="benchmark_data.csv"):
    """Generate test data with configurable columns and distribution for different selectivities"""
    # Create metadata file path
    file_path+=f"_cols{num_columns}.csv"
    meta_file_path = str(file_path) + ".meta"

    # Check if file already exists
    if os.path.exists(file_path):
        file_size_gb = os.path.getsize(file_path) / (1024*1024*1024)
        print(f"Using existing data file: {file_path}")
        print(f"File size: {file_size_gb:.2f} GB")

        # Read column names from metadata file
        if os.path.exists(meta_file_path):
            with open(meta_file_path, 'r') as f:
                columns = json.load(f)
            return file_path, columns
        else:
            # Legacy support - read first row to guess columns
            print("Warning: No metadata file found, guessing column names")
            columns = [f"col_{i}" for i in range(num_columns)]
            with open(meta_file_path, 'w') as f:
                json.dump(columns, f)
            return file_path, columns

    # Calculate rows needed for ~1.5GB file
    bytes_per_row = num_columns * 10  # ~8 bytes per UINT64 + CSV overhead
    target_size = 1.5 * 1024 * 1024 * 1024  # Target 1.5GB
    num_rows = min(num_rows, int(target_size / bytes_per_row))

    # Create data dictionary and column list
    data = {}
    columns = []

    # Generate columns with varied distributions for filter testing
    for i in range(num_columns):
        column_name = f"col_{i}"
        columns.append(column_name)

        #if i % 3 == 0:
        # Uniform distribution - good for predictable selectivity
        data[column_name] = np.random.randint(0, 2**32-1, size=num_rows, dtype=np.uint64)
        #elif i % 3 == 1:
            # Normal distribution (clipped to positive values)
            #values = np.random.normal(2**31, 2**30, size=num_rows)
            #data[column_name] = np.clip(values, 0, 2**32-1).astype(np.uint64)
        #else:
            # Exponential distribution - good for skewed data
            #values = np.random.exponential(2**28, size=num_rows)
            #data[column_name] = values.astype(np.uint64)

    # Create DataFrame and save to CSV without headers
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    df.to_csv(file_path, index=False, header=False)

    # Save column metadata to a separate file
    with open(meta_file_path, 'w') as f:
        json.dump(columns, f)

    file_size_gb = os.path.getsize(file_path) / (1024*1024*1024)
    print(f"Generated {num_rows} rows with {num_columns} columns at {file_path}")
    print(f"File size: {file_size_gb:.2f} GB")
    print(f"Column metadata saved to: {meta_file_path}")

    return file_path, columns

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark data')
    parser.add_argument('--output', type=str, default='benchmark_data.csv', help='Output file path')
    parser.add_argument('--rows', type=int, default=10000000, help='Maximum number of rows')
    parser.add_argument('--columns', type=parse_int_list, default=[10], help='Number of columns')
    args = parser.parse_args()

    # Create output directories if they don't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    for col_count in args.columns:
        generate_data(args.rows, col_count, args.output)