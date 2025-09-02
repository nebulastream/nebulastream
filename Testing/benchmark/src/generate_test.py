#!/usr/bin/env python3
import os
import pandas as pd
import argparse
from pathlib import Path
import numpy as np
import shutil

def generate_test_file(data_file, output_path, result_dir, params):
    """Generate benchmark test file with queries for all parameter combinations"""
    buffer_sizes = params.get('buffer_sizes', [4000])#, 400000, 20000000])
    num_columns_list = params.get('num_columns', [1 ])#, 5, 10])
    accessed_columns_list = params.get('accessed_columns', [1])#, 2])
    function_types = params.get('function_types', ['add'])#, 'exp'])
    selectivities = params.get('selectivities', [5])#, 50, 95])

    # Read column names from data file
    sample_df = pd.read_csv(data_file, nrows=1)
    column_names = sample_df.columns.tolist()

    # Create main test file
    with open(output_path, 'w') as f:
        # Write test file header
        f.write("# name: custom_benchmark.test\n")
        f.write("# description: Generated benchmark test for filter and map operations\n")
        f.write("# groups: [benchmark]\n\n")

        # Source definition
        f.write("# Source definitions\n")
        source_def = "Source bench_data"
        for col in column_names:
            source_def += f" UINT64 {col}"
        source_def += f" FILE\n{os.path.abspath(data_file)}\n\n"
        f.write(source_def)

        # Sink definitions
        f.write("# Sink definitions\n")
        f.write("SINK FilterSink TYPE Checksum UINT64 result\n")
        f.write("SINK MapSink TYPE Checksum UINT64 result\n\n")

    # Create operator directories
    result_dir = Path(result_dir)
    filter_dir = result_dir / "filter"
    map_dir = result_dir / "map"
    filter_dir.mkdir(exist_ok=True, parents=True)
    map_dir.mkdir(exist_ok=True, parents=True)

    query_id = 1
    query_configs = {}

    # For each buffer size, create separate test files and directory structure
    for buffer_size in buffer_sizes:
        filter_buffer_dir = filter_dir / f"bufferSize{buffer_size}"
        map_buffer_dir = map_dir / f"bufferSize{buffer_size}"
        filter_buffer_dir.mkdir(exist_ok=True)
        map_buffer_dir.mkdir(exist_ok=True)

        # Create buffer-specific test files
        filter_test_path = filter_buffer_dir / f"filter_queries_buffer{buffer_size}.test"
        map_test_path = map_buffer_dir / f"map_queries_buffer{buffer_size}.test"

        # Copy header content from main test file
        shutil.copy(output_path, filter_test_path)
        shutil.copy(output_path, map_test_path)

        # Open files for appending queries
        with open(filter_test_path, 'a') as filter_f, open(map_test_path, 'a') as map_f:
            # Generate queries for all parameter combinations for this buffer size
            for num_col in [n for n in num_columns_list if n <= len(column_names)]:
                cols_to_use = column_names[:num_col]

                for access_col in [a for a in accessed_columns_list if a <= num_col]:
                    cols_to_access = cols_to_use[:access_col]

                    # Filter queries with different selectivities
                    for selectivity in selectivities:
                        filter_col = cols_to_access[0]
                        threshold = int((100 - selectivity) * 0.01 * (2**32-1))

                        # Create query directory
                        query_dir = filter_buffer_dir / f"query_sel{selectivity}_cols{num_col}_access{access_col}"
                        query_dir.mkdir(exist_ok=True)

                        # Write query config
                        config = {
                            "query_id": query_id,
                            "buffer_size": buffer_size,
                            "num_columns": num_col,
                            "accessed_columns": access_col,
                            "operator_type": "filter",
                            "selectivity": selectivity,
                            "threshold": threshold
                        }

                        with open(query_dir / "config.txt", 'w') as config_f:
                            for k, v in config.items():
                                config_f.write(f"{k}: {v}\n")

                        # Store config in dictionary for later use
                        query_configs[query_id] = config

                        # Write query to buffer-specific test file
                        query = f"# Query {query_id}: Filter with {selectivity}% selectivity\n"
                        query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, OperatorType: filter, Selectivity: {selectivity}\n"
                        query += f"SELECT {filter_col} AS result FROM bench_data WHERE {filter_col} > UINT64({threshold}) INTO FilterSink;\n"
                        query += "----\n1, 1\n\n"
                        filter_f.write(query)

                        query_id += 1

                    # Map queries with different function types
                    for func_type in function_types:
                        # Create query directory
                        query_dir = map_buffer_dir / f"query_{func_type}_cols{num_col}_access{access_col}"
                        query_dir.mkdir(exist_ok=True)

                        # Write query config
                        config = {
                            "query_id": query_id,
                            "buffer_size": buffer_size,
                            "num_columns": num_col,
                            "accessed_columns": access_col,
                            "operator_type": "map",
                            "function_type": func_type
                        }

                        with open(query_dir / "config.txt", 'w') as config_f:
                            for k, v in config.items():
                                config_f.write(f"{k}: {v}\n")

                        # Store config in dictionary for later use
                        query_configs[query_id] = config

                        # Write query to buffer-specific test file
                        query = f"# Query {query_id}: Map with {func_type} function\n"
                        query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, OperatorType: map, FunctionType: {func_type}\n"

                        if func_type == 'add' and len(cols_to_access) >= 2:
                            expression = f"({cols_to_access[0]} + {cols_to_access[1]})"
                        elif func_type == 'exp':
                            expression = f"({cols_to_access[0]} * {cols_to_access[0]})"
                        else:
                            expression = f"({cols_to_access[0]} * UINT64(2))"

                        query += f"SELECT {expression} AS result FROM bench_data INTO MapSink;\n"
                        query += "----\n1, 1\n\n"
                        map_f.write(query)

                        query_id += 1

    # Write query mapping to a file for later reference
    with open(result_dir / "query_mapping.txt", 'w') as f:
        for qid, config in query_configs.items():
            f.write(f"Query {qid}: {config}\n")

    print(f"Generated test files with {query_id-1} queries")
    print(f"Created organized directory structure in {result_dir}")
    return query_id-1, query_configs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark test file')
    parser.add_argument('--data', required=True, help='Path to benchmark data CSV')
    parser.add_argument('--output', required=True, help='Output test file path')
    parser.add_argument('--result-dir', required=True, help='Result directory for organized structure')
    args = parser.parse_args()

    # Customizable parameters
    params = {
        'buffer_sizes': [4000, 400000, 20000000],
        'num_columns': [1, 5, 10],
        'accessed_columns': [1, 2],
        'function_types': ['add', 'exp'],
        'selectivities': [5, 50, 95]
    }

    generate_test_file(args.data, args.output, args.result_dir, params)