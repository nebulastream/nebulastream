#!/usr/bin/env python3
import os
import pandas as pd
import argparse
from pathlib import Path
import numpy as np
import shutil
import json
from benchmark_system import parse_int_list

def generate_test_file(data_file, output_path, result_dir, params):
    """Generate benchmark test file with queries for all parameter combinations"""
    buffer_sizes = params.get('buffer_sizes', [4000, 400000, 20000000])
    num_columns_list = params.get('num_columns', [1, 5, 10])
    accessed_columns_list = params.get('accessed_columns', [1, 2])
    function_types = params.get('function_types', ['add', 'exp'])
    selectivities = params.get('selectivities', [5, 50, 95])

    docker_base_path= "/tmp/nebulastream/Testing/benchmark/benchmark_results/data"

    base_name = os.path.basename(data_file).split('_cols')[0]
    docker_data_path = docker_base_path + f"/{base_name}_cols{{}}.csv"

    print(f"Using output test file: {output_path}")
    # Read column names from data file
    meta_file = "benchmark_results/data/" + base_name + f"_cols{num_columns_list[-1]}.csv"  + ".meta"
    print(f"Reading metadata from: {meta_file}")
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as f:
            column_names = json.load(f)
    else:
        # Fallback if metadata file doesn't exist
        print("Warning: No metadata file found, generating default column names")
        column_names = [f"col_{i}" for i in range(num_columns_list[-1])]


    # Create main test file
    with open(output_path, 'w') as f:
        # Write test file header
        f.write("# name: custom_benchmark.test\n")
        f.write("# description: Generated benchmark test for filter and map operations\n")
        f.write("# groups: [benchmark]\n\n")

        # Source definition with Docker-compatible path
        f.write("# Source definitions\n")
        for num_cols in num_columns_list:
            docker_path = str(docker_data_path).format(num_cols)
            #if not os.path.exists(docker_path):
                #print(f"Warning: Data file for {num_cols} columns does not exist at {docker_path}")
            source_def = f"Source bench_data{num_cols}"
            names = column_names[:num_cols]#TODO: not depend on same column names?
            for col in names:
                source_def += f" UINT64 {col}"
            source_def += f" FILE\n{docker_path}\n\n"
            f.write(source_def)

        # Sink definitions
        f.write("# Sink definitions\n")
        for num_cols in num_columns_list:
            sink_def= f"SINK AllSink{num_cols} TYPE Checksum"
            names = column_names[:num_cols]
            for col in names:
                sink_def += f" UINT64 bench_data{num_cols}${col}"
            f.write(sink_def + "\n")

        for acc_cols in accessed_columns_list:
            #smallest_num_cols = np.min([n for n in num_columns_list if n >= acc_cols], default=None)
            #if smallest_num_cols is None:
                #continue
            #source_qualifier = f"bench_data{smallest_num_cols}$"
            if acc_cols != 1:
                acc_cols -= 1
                if acc_cols < 2:
                    continue
            sink = f"SINK MapSink{acc_cols} TYPE Checksum UINT64 result1"
            for col_index in range(2,acc_cols + 1):#)column_names[1:acc_cols]:
                sink +=(f" UINT64 result{col_index}")
            f.write(sink + "\n")
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
        map_queries=0
        filter_queries=0
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
                        filter_queries+=1
                        # Calculate individual selectivity needed for each column
                        # For N columns with AND, each needs selectivity^(1/N) to achieve target selectivity
                        individual_selectivity = selectivity**(1.0/access_col)

                        # Calculate threshold for this individual selectivity
                        threshold = int((100 - individual_selectivity) * 0.01 * (2**32-1))


                        # Create query directory
                        query_dir = filter_buffer_dir / f"query_sel{selectivity}_cols{num_col}_access{access_col}"
                        query_dir.mkdir(exist_ok=True)

                        # Write query config
                        config = {
                            "query_id": query_id,
                            "test_id": filter_queries,
                            "buffer_size": buffer_size,
                            "num_columns": num_col,
                            "accessed_columns": access_col,
                            "operator_type": "filter",
                            "selectivity": selectivity,
                            "individual_selectivity": individual_selectivity,
                            "threshold": threshold,
                        }

                        with open(query_dir / "config.txt", 'w') as config_f:
                            for k, v in config.items():
                                config_f.write(f"{k}: {v}\n")

                        # Store config in dictionary for later use
                        query_configs[query_id] = config

                        # Write query to buffer-specific test file
                        query = f"# Query {query_id}: Filter with {selectivity}% selectivity\n"
                        query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, OperatorType: filter, Selectivity: {selectivity}\n"

                        query += (f"SELECT * FROM bench_data{num_col} WHERE ({filter_col} > UINT64({threshold})")
                        for col_name in cols_to_access[1:]:
                            query += (f" AND {col_name} > UINT64({threshold})")

                        query += (f") INTO AllSink{num_col};\n")
                        query += "----\n1, 1\n\n"
                        filter_f.write(query)

                        query_id += 1

                    # Map queries with different function types
                    for func_type in function_types:
                        map_queries+=1
                        # Create query directory
                        query_dir = map_buffer_dir / f"query_{func_type}_cols{num_col}_access{access_col}"
                        query_dir.mkdir(exist_ok=True)

                        # Write query config
                        config = {
                            "query_id": query_id,
                            "test_id": map_queries,
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
                        asterisk = "*" if func_type == 'add' else "+"
                        expression_template = f"{{}} {asterisk} {{}} AS result{{}}"
                        col_index=1
                        if len(cols_to_access)>1:
                            query += f"SELECT {expression_template.format(cols_to_access[0], cols_to_access[1], col_index)}"
                        else:
                            query += f"SELECT {expression_template.format(cols_to_access[0], cols_to_access[0], col_index)}"

                        for col_index in range(2,len(cols_to_access)):#cols_to_access[1:]:
                            query += f", {expression_template.format(cols_to_access[col_index-1], cols_to_access[col_index], col_index)}"
                        if access_col != 1:
                            query += f" FROM bench_data{num_col} INTO MapSink{access_col - 1};\n"#two accessed fields result in one output field
                        else:
                            query += f" FROM bench_data{num_col} INTO MapSink{access_col};\n"
                        query += "----\n1, 1\n\n"
                        map_f.write(query)

                        query_id += 1

    # Write query mapping to a file for later reference
    with open(result_dir / "query_mapping.txt", 'w') as f:
        for qid, config in query_configs.items():
            f.write(f"Query {qid}: {config}\n")

    print(f"Generated test files with {query_id-1} queries")
    print(f"Created organized directory structure in {result_dir}")
    print(f"Using Docker-compatible data path: {docker_data_path}")
    return query_id-1, query_configs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark test file')
    parser.add_argument('--data', required=True, help='Path to benchmark data CSV')
    parser.add_argument('--output', required=True, help='Output test file path')
    parser.add_argument('--result-dir', required=True, help='Result directory for organized structure')
    parser.add_argument('--columns', type=parse_int_list, default=[10], help='List of number of columns to use')
    args = parser.parse_args()

    # Customizable parameters
    params = {
        'buffer_sizes': [4000, 40000, 400000, 4000000, 10000000, 20000000],
        'num_columns': args.columns, #, 5, 10], TODO: correctly use more than 2 columns
        'accessed_columns': [1, 2, 5, 10],
        'function_types': ['add', 'exp'],
        'selectivities': [5, 45, 85],# 15, 25, 35, 45, 50, 55, 65, 75, 85, 95]
    }

    generate_test_file(args.data, args.output, args.result_dir, params)