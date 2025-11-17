#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import argparse
import json
from pathlib import Path
from utils import parse_int_list, parse_str_list
import re

def generate_data(num_rows=10000000, num_columns=10, num_groups=None, data_size="", id_data_type='',  file_path=None):
    """Generate test data with configurable columns, windows, and groups for different selectivities."""
    if not file_path:
        file_path = "benchmark_data"
        print("Generating data with parameters")
        file_path += f"_cols{num_columns}"
        if num_groups is not None:
            if id_data_type != '':
                file_path += f"_groups{num_groups}_idtype{id_data_type}"
            else:
                file_path += f"_groups{num_groups}"
        file_path += ".csv"
    else:
        file_path = str(file_path)
        id_match = re.search(r'idtype(\d+)', file_path)
        if id_match:
            id_data_type = id_match.group(1)
        groups_match = re.search(r'groups(\d+)', file_path)
        if groups_match:
            num_groups = int(groups_match.group(1))
        cols_match = re.search(r'cols(\d+)', file_path)
        if cols_match:
            num_columns = int(cols_match.group(1))
        rows_match = re.search(r'rows(\d+)', file_path)
        if rows_match:
            num_rows = int(rows_match.group(1))

        data_match = re.search(r'int(\d+)', file_path)
        if data_match:
            data_size = int(rows_match.group(1))

    meta_file_path = str(file_path) + (".meta")

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

        if id_data_type != '':
            columns.append("id")
            dtype = np.uint8
            if '32' in id_data_type:
                dtype = np.uint32
            elif '64' in id_data_type:
                dtype = np.uint64
            elif '16' in id_data_type:
                dtype = np.uint16
            else:
                print(f"Unknown id_data_type {id_data_type}, defaulting to uint8")


            data["id"] = np.random.randint(0, num_groups, size=num_rows, dtype=dtype)

    # Generate other columns with up to num_groups unique values
    for i in range(num_columns):
        column_name = f"col_{i}"
        columns.append(column_name)
        if data_size == "":
            data[column_name] = np.random.randint(0, 2 ** 32 - 1, size=num_rows, dtype=np.uint64)
        else:
            data[column_name] = np.random.randint(0, 2 ** 7 - 1, size=num_rows, dtype=np.uint8)

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
    parser.add_argument('--id-data-types', type=parse_str_list, default = ['', "64"], help='Data type for id column (e.g., uint32, uint64), empty for no id column')
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    for col_count in args.columns:
        for groups in args.groups:
            for data_type in args.id_data_types:
                generate_data(args.rows, col_count, groups, data_type, args.output)
        generate_data(args.rows, col_count, None, file_path= args.output)

