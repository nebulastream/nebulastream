#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import argparse
import json
from pathlib import Path
from benchmark_system import parse_int_list

def generate_data(num_rows=10000000, num_columns=10, num_groups=None, file_path="benchmark_data.csv"):
    """Generate test data with configurable columns, windows, and groups for different selectivities."""

    file_path += f"_cols{num_columns}"
    if num_groups is not None:
        file_path += f"_groups{num_groups}.csv"
    meta_file_path = str(file_path) + ".meta"

    # Check if file already exists
    if os.path.exists(file_path):
        file_size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
        print(f"Using existing data file: {file_path}")
        print(f"File size: {file_size_gb:.2f} GB")
        return file_path, None

    # Generate data
    data = {}
    columns = []
    if num_groups is not None:
        columns = ["timestamp"]

        # Generate timestamp column
        data["timestamp"] = np.arange(num_rows)
        data["id"] = np.random.randint(0, num_groups, size=num_rows, dtype=np.uint64)

    # Generate other columns with up to num_groups unique values
    for i in range(num_columns):
        column_name = f"col_{i}"
        columns.append(column_name)
        data[column_name] = np.random.randint(0, 2 ^ 32 - 1, size=num_rows, dtype=np.uint64)

    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    df.to_csv(file_path, index=False, header=False)

    # Save column metadata
    with open(meta_file_path, 'w') as f:
        json.dump(columns, f)

    file_size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
    print(f"Generated {num_rows} rows with {num_columns + 1} columns at {file_path}")
    print(f"File size: {file_size_gb:.2f} GB")
    print(f"Column metadata saved to: {meta_file_path}")

    return file_path, columns

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark data')
    parser.add_argument('--output', type=str, default='benchmark_data.csv', help='Output file path')
    parser.add_argument('--rows', type=int, default=10000000, help='Maximum number of rows')
    parser.add_argument('--columns', type=parse_int_list, default=[10], help='Number of columns (including timestamp)')
    parser.add_argument('--groups', type=parse_int_list, default=[100], help='Number of unique groups per column')
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    for col_count in args.columns:
        for groups in args.groups:
            generate_data(args.rows, col_count, groups, args.output)
        generate_data(args.rows, col_count, None, file_path= args.output)

