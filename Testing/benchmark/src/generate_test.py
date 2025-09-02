#!/usr/bin/env python3
import os
import pandas as pd
import argparse
from pathlib import Path
import numpy as np

def generate_test_file(data_file, output_path, params):
    """Generate benchmark test file with queries for all parameter combinations"""
    buffer_sizes = params.get('buffer_sizes', [4000, 400000, 20000000])
    num_columns_list = params.get('num_columns', [1, 5, 10])
    accessed_columns_list = params.get('accessed_columns', [1, 2])
    function_types = params.get('function_types', ['add', 'exp'])
    selectivities = params.get('selectivities', [5, 50, 95])

    # Read column names from data file
    sample_df = pd.read_csv(data_file, nrows=1)
    column_names = sample_df.columns.tolist()

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

        query_id = 1

        # Generate queries for all parameter combinations
        for buffer_size in buffer_sizes:
            for num_col in [n for n in num_columns_list if n <= len(column_names)]:
                cols_to_use = column_names[:num_col]

                for access_col in [a for a in accessed_columns_list if a <= num_col]:
                    cols_to_access = cols_to_use[:access_col]

                    # Filter queries with different selectivities
                    for selectivity in selectivities:
                        filter_col = cols_to_access[0]

                        # Calculate threshold based on selectivity
                        threshold = int((100 - selectivity) * 0.01 * (2**32-1))

                        query = f"# Query {query_id}: Filter with {selectivity}% selectivity\n"
                        query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, OperatorType: filter, Selectivity: {selectivity}\n"
                        query += f"SELECT {filter_col} AS result FROM bench_data WHERE {filter_col} > UINT64({threshold}) INTO FilterSink;\n"
                        query += "----\n1, 1\n\n"
                        f.write(query)
                        query_id += 1

                    # Map queries with different function types
                    for func_type in function_types:
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
                        f.write(query)
                        query_id += 1

        print(f"Generated test file with {query_id-1} queries at {output_path}")
        return query_id-1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark test file')
    parser.add_argument('--data', required=True, help='Path to benchmark data CSV')
    parser.add_argument('--output', required=True, help='Output test file path')
    args = parser.parse_args()

    # Customizable parameters
    params = {
        'buffer_sizes': [4000, 400000, 20000000],
        'num_columns': [1, 5, 10],
        'accessed_columns': [1, 2],
        'function_types': ['add', 'exp'],
        'selectivities': [5, 50, 95]
    }

    generate_test_file(args.data, args.output, params)