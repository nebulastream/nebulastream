#!/usr/bin/env python3
import os
import pandas as pd
import argparse
from pathlib import Path
import numpy as np
import shutil
import json
from utils import parse_int_list, parse_str_list

def swap_config_to_strategy(swap_config):
    """Convert swap configuration to layout strategy name"""
    if swap_config == [False, False, False]:  # No swaps
        return "ALL_ROW"
    elif swap_config == [True, True, False]:
        return "ALL_COL"
    elif swap_config == [True, False, False]:  # Just first swap
        return "FIRST"
    elif swap_config == [False, True, False]:  # Just middle swap
        return "SECOND"
    else:
        return "CUSTOM"  # For other configurations

def generate_double_operator_query(query_id, test_id, buffer_size, num_cols, access_cols, op_chain, swap_config, result_dir, func_type, selectivity):
    """Generate a query with a chain of two operators and configurable swaps"""

    # Create strategy-specific directory in double_operator
    strategy = swap_config
    chain_str = '_'.join(op_chain)

    # For directory structure
    strategy_dir = result_dir / strategy
    strategy_dir.mkdir(exist_ok=True)

    buffer_dir = strategy_dir / f"bufferSize{buffer_size}"
    buffer_dir.mkdir(exist_ok=True)

    # Create operator chain directory
    chain_dir = buffer_dir / chain_str
    chain_dir.mkdir(exist_ok=True)

    # Calculate threshold for filters (use same threshold calculation as single operators)
    #selectivity = 50  # Default selectivity
    #threshold = int((100 - selectivity) * 0.01 * (2**32-1))
    #func_type = "add"  # Default function type

    # Determine function type and selectivity based on operator chain
    if "filter" in op_chain:
        selectivity_str = f"sel{selectivity}"
    else:
        selectivity_str = ""

    if "map" in op_chain:
        func_type_str = f"{func_type}"
    else:
        func_type_str = ""

    # Build query directory name with proper parameters
    query_params = []
    if selectivity_str:
        query_params.append(selectivity_str)
    if func_type_str:
        query_params.append(func_type_str)
    query_params.append(f"cols{num_cols}")
    query_params.append(f"acc{access_cols}")
    query_params.append(strategy)

    query_dir_name = "query_" + "_".join(query_params)
    query_dir = chain_dir / query_dir_name
    query_dir.mkdir(exist_ok=True)

    # Prepare test file path - keep test files in strategy-specific locations
    test_file = buffer_dir / f"{chain_str}_{strategy}_queries_buffer{buffer_size}.test"

    # Calculate threshold for filters (use same threshold calculation as single operators)
    #selectivity = 50  # Default selectivity
    threshold = int((100 - selectivity) * 0.01 * (2**32-1))

    # Generate query SQL
    query = f"# Query {test_id}: Chain {chain_str} with strategy {strategy}\n"
    query += f"# BufferSize: {buffer_size}, Operators: {chain_str}, num_cols: {num_cols}, acc_cols: {access_cols}, func{func_type} sel: {selectivity}, th: {threshold}\n"

    # Determine appropriate sink based on number of accessed columns
    if access_cols == 1:
        sink = "MixedSink1"
    else:
        sink = "MixedSink2"

    if op_chain == ['map', 'filter']:
        # First map then filter
        if access_cols == 1:
            inner_expr = f"col_0 + col_0 AS result1"
            outer_expr = f"result1 > UINT64({threshold})"
        else:
            inner_exprs = [f"col_{i} + col_{i} AS result{i+1}" for i in range(min(access_cols, num_cols))]
            inner_expr = ", ".join(inner_exprs)
            outer_expr = " AND ".join([f"result{i+1} > UINT64({threshold})" for i in range(min(access_cols, num_cols))])

        # Structure query properly with nested SELECT statements
        query += f"SELECT {'*' if access_cols == 1 else 'result1, result2'} FROM (SELECT {inner_expr} FROM bench_data{num_cols}) WHERE ({outer_expr}) INTO {sink};\n"

    elif op_chain == ['filter', 'map']:
        # First filter then map
        filter_expr = " AND ".join([f"col_{i} > UINT64({threshold})" for i in range(min(access_cols, num_cols))])

        if access_cols == 1:
            map_expr = f"col_0 + col_0 AS result1"
        else:
            map_exprs = [f"col_{i} + col_{i} AS result{i+1}" for i in range(min(access_cols, num_cols))]
            map_expr = ", ".join(map_exprs)

        # Structure query properly with nested SELECT statements
        query += f"SELECT {map_expr} FROM (SELECT * FROM bench_data{num_cols} WHERE ({filter_expr})) INTO {sink};\n"


    # Store configuration
    config = {
        'query_id': query_id,
        'test_id': test_id,
        'buffer_size': buffer_size,
        'num_columns': num_cols,
        'accessed_columns': access_cols,
        'operator_chain': op_chain,
        'swap_strategy': strategy,
        'selectivity': selectivity if 'filter' in op_chain else None,
        'function_type': func_type if 'map' in op_chain else None
    }

    with open(query_dir / "config.txt", 'w') as f:
        for k, v in config.items():
            if v is not None:
                f.write(f"{k}: {v}\n")

    return query, config, test_file
def generate_test_file(data_file, result_dir, params, run_options='all'):
    """Generate benchmark test file with queries for all parameter combinations"""
    buffer_sizes = params.get('buffer_sizes', [4000, 400000, 20000000])
    num_columns_list = params.get('num_columns', [1, 5, 10])
    accessed_columns_list = params.get('accessed_columns', [1, 2])
    function_types = params.get('function_types', ['add', 'exp'])
    selectivities = params.get('selectivities', [5, 50, 95])
    agg_functions = params.get('agg_functions', ['count', 'sum', 'avg', 'min', 'max'])
    output_path = "/home/user/CLionProjects/nebulastream/Testing/benchmark/benchmark_results/data/benchmark.test"
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
        f.write("SINK MixedSink1 TYPE Checksum UINT64 result1\n")
        #f.write("SINK MixedSink2 TYPE Checksum UINT64 result1 UINT64 result2\n\n")
        for acc_cols in accessed_columns_list:
            if acc_cols>1:
                sink= f"SINK MixedSink{acc_cols} TYPE Checksum"
                for col_index in range(1,acc_cols + 1):#)column_names[1:acc_cols]:
                    sink +=(f" UINT64 result{col_index}")
                f.write(sink + "\n")
        f.write("\n")
    # Create operator directories

    result_dir = Path(result_dir)
    double_operator_dir = result_dir / "double_operator"
    filter_dir = result_dir / "filter"
    map_dir = result_dir / "map"
    agg_dir = result_dir / "aggregation"



    query_id = 1
    query_configs = {}

    if run_options == 'all' or run_options == 'single': #TODO: refactor in own function
        filter_dir.mkdir(exist_ok=True, parents=True)
        map_dir.mkdir(exist_ok=True)
        agg_dir.mkdir(exist_ok=True)
        # For each buffer size, create separate test files and directory structure
        for buffer_size in buffer_sizes:
            filter_buffer_dir = filter_dir / f"bufferSize{buffer_size}"
            map_buffer_dir = map_dir / f"bufferSize{buffer_size}"
            agg_buffer_dir = agg_dir / f"bufferSize{buffer_size}"
            filter_buffer_dir.mkdir(exist_ok=True)
            map_buffer_dir.mkdir(exist_ok=True)
            agg_buffer_dir.mkdir(exist_ok=True)

            map_queries=0
            filter_queries=0
            agg_queries=0
            # Open files for appending queries
            #with open(filter_test_path, 'a') as filter_f, open(map_test_path, 'a') as map_f, open(agg_test_path, 'a') as agg_f:
                #TODO: move files into per query then write to query dir
                # Generate queries for all parameter combinations for this buffer size
            for num_col in [n for n in num_columns_list if n <= len(column_names)]:
                cols_to_use = column_names[:num_col]

                for access_col in [a for a in accessed_columns_list if a <= num_col]:
                    cols_to_access = cols_to_use[:access_col]

                    # Filter queries with different selectivities
                    for selectivity in selectivities:
                        if ['filter'] not in params['operator_chains']:
                            break
                        for data_size in  [True, False]:
                            #add query dir to file path
                            query_dir = filter_buffer_dir
                            query = f"query_sel{selectivity}_cols{num_col}_access{access_col}"
                            if not data_size:
                                query += "_uint8"
                            query_dir = query_dir / query
                            query_dir.mkdir(exist_ok=True)
                            filter_test_path = query_dir / f"filter_queries_buffer{buffer_size}.test"
                            with open(filter_test_path, 'w') as filter_f:
                                filter_col = cols_to_access[0]
                                filter_queries+=1
                                # Calculate individual selectivity needed for each column
                                # For N columns with AND, each needs nth root(selectivity) to achieve target selectivity
                                #95% selectivity -> 5% data remaining
                                individual_selectivity = selectivity**(1.0/access_col)

                                # Calculate threshold for this individual selectivity
                                threshold = int((100 - individual_selectivity) * 0.01 * (2**32-1))# 95% of 4 mrd
                                if not data_size:
                                    threshold = int((100 - individual_selectivity) * 0.01 * (2**7-1))# 95% of 4 mrd

                                # column > threshold = 100% - 95% = 5% remaining
                                #3:
                                data_size_size = "" if data_size else "_uint8"
                                # Write query config
                                config = {
                                    "query_id": query_id,
                                    "test_id": filter_queries,
                                    "buffer_size": buffer_size,
                                    "num_columns": num_col,
                                    "accessed_columns": access_col,
                                    "operator_chain": ["filter"],
                                    "swap_strategy": "USE_SINGLE_LAYOUT",
                                    "selectivity": selectivity,
                                    "individual_selectivity": individual_selectivity,
                                    "threshold": threshold,
                                    "data_size": data_size_size,
                                    'data_name': base_name + f"_cols{num_col}{data_size_size}.csv"
                                }

                                with open(query_dir / "config.txt", 'w') as config_f:
                                    for k, v in config.items():
                                        config_f.write(f"{k}: {v}\n")

                                # Store config in dictionary for later use
                                query_configs[query_id] = config
                                header = build_header(f"bench_data{num_col}", f"AllSink{num_col}", column_names[:num_col], docker_data_path, data_size)

                                # Write query to buffer-specific test file
                                query = f"# Query {query_id}: Filter with {selectivity}% selectivity\n"
                                query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, OperatorType: filter, Selectivity: {selectivity}\n"

                                query += (f"SELECT * FROM bench_data{num_col} WHERE ({filter_col} > UINT64({threshold})")
                                for col_name in cols_to_access[1:]:
                                    query += (f" AND {col_name} > UINT64({threshold})")

                                query += (f") INTO AllSink{num_col};\n")
                                query += "----\n1, 1\n\n"
                                filter_f.write(header + query)

                                query_id += 1

                    # Map queries with different function types
                    for func_type in function_types:
                        if ['map'] not in params['operator_chains']:
                            break
                        for data_size in  [True, False]:
                            query_dir = map_buffer_dir
                            query = f"query_{func_type}_cols{num_col}_access{access_col}"
                            if not data_size:
                                query += "_uint8"
                            query_dir = query_dir / query
                            query_dir.mkdir(exist_ok=True)
                            map_test_path = query_dir  / f"map_queries_buffer{buffer_size}.test"
                            with open(map_test_path, 'w') as map_f:

                                map_queries+=1
                                data_size_size = "" if data_size else "_uint8"

                                # Write query config
                                config = {
                                    "query_id": query_id,
                                    "test_id": map_queries,
                                    "buffer_size": buffer_size,
                                    "num_columns": num_col,
                                    "accessed_columns": access_col,
                                    "operator_chain": ["map"],
                                    "swap_strategy": "USE_SINGLE_LAYOUT",
                                    "function_type": func_type,
                                    "data_size": data_size_size,
                                    "data_name": base_name + f"_cols{num_col}{data_size_size}.csv"
                                }

                                with open(query_dir / "config.txt", 'w') as config_f:
                                    for k, v in config.items():
                                        config_f.write(f"{k}: {v}\n")

                                # Store config in dictionary for later use
                                query_configs[query_id] = config

                                # Store config in dictionary for later use
                                sink = f"AllSink{num_col}"
                                header = build_header(f"bench_data{num_col}", sink,  column_names[:num_col], docker_data_path, data_size)

                                # Write query to buffer-specific test file
                                query = f"# Query {query_id}: Map with {func_type} function\n"
                                query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, OperatorType: map, FunctionType: {func_type}\n"
                                asterisk = "+" if func_type == 'add' else "*"
                                expression_template = f"{{}} {asterisk} UINT64(2) AS {{}}"
                                col_index=1
                                if len(cols_to_access)>1:
                                    query += f"SELECT {expression_template.format(cols_to_access[0], cols_to_access[1], col_index)}"
                                else:
                                    query += f"SELECT {expression_template.format(cols_to_access[0], cols_to_access[0], col_index)}"

                                for col_index in range(2,len(cols_to_access)):#cols_to_access[1:]:
                                    query += f", {expression_template.format(cols_to_access[col_index-1], cols_to_access[col_index], col_index)}"
                                for col_index in range (len(cols_to_access), num_col):
                                    query += f", col_{col_index}"
                                query += f" FROM bench_data{num_col} INTO {sink};\n"#two accessed fields result in one output field
                                query += "----\n1, 1\n\n"
                                map_f.write(header + query)

                                query_id += 1

                    # Aggregation queries
                    for agg_func in agg_functions:
                        if ['aggregation'] not in params['operator_chains']:
                            break
                        for window_size in params['window_sizes']:
                            for num_groups in params['num_groups']:
                                for id_type in params['id_data_types']:
                                    if id_type != '':
                                        max_value = 2**int(id_type)-1
                                        if num_groups > max_value:
                                            #print(f"Skipping aggregation query with id_data_type {id_type} and num_groups {num_groups} (exceeds max value {max_value})")
                                            continue
                                    query_dir = agg_buffer_dir / f"query_{agg_func}_cols{num_col}_access{access_col}_win-size{window_size}_num-groups{num_groups}"
                                    if id_type != '':
                                        query_dir = agg_buffer_dir / f"query_{agg_func}_cols{num_col}_access{access_col}_win-size{window_size}_num-groups{num_groups}_idtype{id_type}"

                                    query_dir.mkdir(exist_ok=True)
                                    agg_test_path = query_dir / f"agg_queries_buffer{buffer_size}.test"

                                    with open(agg_test_path, 'w') as agg_f:
                                        agg_queries+=1
                                        # Create query directory



                                        source = f"bench_data{num_col}_groups{num_groups}"
                                        if id_type != '':
                                            source += f"_idtype{id_type}"

                                        header = build_header(source, f"AggSink{num_col}", column_names[:num_col], docker_data_path)

                                        # Write query config
                                        config = {
                                            "query_id": query_id,
                                            "test_id": agg_queries,
                                            "buffer_size": buffer_size,
                                            "num_columns": num_col,
                                            "accessed_columns": access_col,
                                            "operator_chain": ["aggregation"],
                                            "swap_strategy": "USE_SINGLE_LAYOUT",
                                            "aggregation_function": agg_func,
                                            "window_size": window_size,
                                            "num_groups": num_groups,
                                            "id_data_type": id_type,
                                            'data_name': base_name + f"_cols{num_col}_groups{num_groups}" + (f"_idtype{id_type}" if id_type != '' else '') + ".csv"
                                        }
                                        with open(query_dir / "config.txt", 'w') as config_f:
                                            for k, v in config.items():
                                                config_f.write(f"{k}: {v}\n")

                                        # Store config in dictionary for later use
                                        query_configs[query_id] = config

                                        # Write query to buffer-specific test file
                                        query = f"# Query {query_id}: Aggregation with {agg_func} function\n"
                                        query += f"# BufferSize: {buffer_size}, NumColumns: {num_col}, AccessedColumns: {access_col}, WindowSize: {window_size}, NumGroups: {num_groups}, ID_data_type: {id_type}, AggregationFunction: {agg_func}\n"

                                        query += f"SELECT "#timestamp"
                                        if id_type != '':
                                            query += "id, " #", id"
                                        query += (f"{agg_func}({cols_to_access[0]}) AS {cols_to_access[0]}")
                                        for col_name in cols_to_access[1:]:
                                            query += f" ,{agg_func}({col_name}) AS {col_name}"
                                        query += " \n"
                                        query += f"FROM {source} \n"

                                        if id_type != '':

                                            query += f"GROUP BY id\n"

                                        query += f"WINDOW TUMBLING(timestamp, SIZE {window_size} MS)\n"

                                        query += f"INTO AggSink{num_col};\n"
                                        query += "----\n1, 1\n\n"

                                        #TODO: add proper sink defs, try double
                                        agg_f.write(header + query)

                                        query_id += 1



    if run_options == 'all' or run_options == 'double':
        double_operator_dir.mkdir(exist_ok=True, parents=True)
        # Generate double operator queries
        for buffer_size in buffer_sizes:
            for op_chain in params['operator_chains']:
                # Skip single operators for double operator section
                if len(op_chain) < 2:
                    continue

                for swap_config in params['swap_strategy']:
                    test_id=1
                    for num_col in [n for n in num_columns_list if n <= len(column_names)]:
                        for access_col in [a for a in accessed_columns_list if a <= num_col]:
                            # Generate double operator query
                            for func_type in function_types if 'map' in op_chain else [None]:
                                for selectivity in selectivities if 'filter' in op_chain else [None]:
                                    # Generate query only if parameters are relevant
                                    if ('map' in op_chain and func_type is None) or ('filter' in op_chain and selectivity is None):
                                        continue
                                    #TODO: implement agg

                                    # Calculate individual selectivity needed for each column
                                    individual_selectivity = None
                                    threshold = None
                                    ##if 'filter' in op_chain:
                                        #individual_selectivity = selectivity**(1.0/access_col)
                                       # threshold = int((100 - individual_selectivity) * 0.01 * (2**32-1))
                                    query, config, test_file = generate_double_operator_query(
                                        query_id, test_id, buffer_size, num_col, access_col, op_chain,
                                        swap_config, double_operator_dir, func_type, selectivity
                                    )


                                    # Append to test file or create if doesn't exist
                                    if not os.path.exists(test_file):
                                        # Copy header content from main test file
                                        shutil.copy(output_path, test_file)

                                    with open(test_file, 'a') as f:
                                        f.write(query)
                                        f.write("----\n1, 1\n\n")

                                    # Store query config and increment IDs properly
                                    query_configs[query_id] = config
                                    test_id += 1
                                    query_id += 1

    # Write query mapping to a file for later reference
    with open(result_dir / "query_configs.csv", 'w') as f:
        cols = ['query_id', 'test_id', 'buffer_size', 'num_columns', 'accessed_columns',
                'operator_chain', 'swap_strategy', 'data_size', 'selectivity', 'individual_selectivity',
                'threshold', 'function_type', 'aggregation_function', 'window_size',
                'num_groups', 'id_data_type']
        f.write(','.join(cols) + '\n')
        for qid, config in query_configs.items():
            row = [str(config.get(col, '')) for col in cols]
            f.write(','.join(row) + '\n')

    print(f"Generated test files with {query_id-1} queries")
    print(f"Created organized directory structure in {result_dir}")
    print(f"Using Docker-compatible data path: {docker_data_path}")
    return query_id-1, query_configs

def generate_operator_chain_query(query_id, buffer_size, num_cols, op_chain, swap_config, result_dir):
    """Generate a query with a chain of operators and configurable swaps"""

    # Create query directory structure
    swap_config_str = ''.join(['S' if s else 'N' for s in swap_config])
    chain_str = '_'.join(op_chain)
    query_dir = result_dir / f"query_{chain_str}_swap{swap_config_str}_cols{num_cols}"
    query_dir.mkdir(exist_ok=True)

    # Generate query SQL with appropriate swap operators
    query = f"# Query {query_id}: Chain {chain_str} with swap config {swap_config_str}\n"
    query += "SELECT "

    # Configure source and first swap if needed
    source = f"bench_data{num_cols}"
    if swap_config[0]:
        query += "SWAP_LAYOUT "  # First swap (after source)

    # Add operators and intermediate swaps
    last_op_idx = len(op_chain) - 1
    for i, op in enumerate(op_chain):
        if op == 'filter':
            query += f"FILTER col_0 > UINT64({threshold}) "
        else:  # map
            query += f"MAP (col_0 + col_1) AS result{i+1} "

    query += f"FROM {source} INTO MapSink;\n"

    # Store configuration
    config = {
        'query_id': query_id,
        'buffer_size': buffer_size,
        'operator_chain': op_chain,
        'swap_strategy': swap_config,
        'num_columns': num_cols
    }

    with open(query_dir / "config.txt", 'w') as f:
        for k, v in config.items():
            f.write(f"{k}: {v}\n")

    return query, config

def build_sources(docker_data_path, num_columns_list, column_names, params):
    source_def=""
    # Source definition with Docker-compatible path
    source_def += "# Source definitions\n"
    for num_cols in num_columns_list:
        docker_path = str(docker_data_path).format(num_cols)
        #if not os.path.exists(docker_path):
        #print(f"Warning: Data file for {num_cols} columns does not exist at {docker_path}")
        source_def += f"Source bench_data{num_cols}"
        names = column_names[:num_cols]#TODO: not depend on same column names?
        for col in names:
            source_def += f" UINT64 {col}"
        source_def += f" FILE\n{docker_path}\n\n"
        for num_groups in params['num_groups']:
            for id_type in params['id_data_types']:
                if id_type != '':
                    source_def += f"Source bench_data{num_cols}_groups{num_groups}_idtype{id_type}"
                    source_def += f" UINT64 timestamp UINT{id_type} id"
                    for col in names:
                        source_def += f" UINT64 {col}"
                    source_def += f" FILE\n{str(docker_data_path).format( f"{num_cols}_groups{num_groups}_idtype{id_type}")}\n\n"
                else:
                    source_def += f"Source bench_data{num_cols}_groups{num_groups}"
                    source_def += f" UINT64 timestamp"
                    for col in names:
                        source_def += f" UINT64 {col}"
                    source_def += f" FILE\n{str(docker_data_path).format(f"{num_cols}_groups{num_groups}")}\n\n"

        return source_def

def build_sinks(num_cols, id_type, column_names, num_columns_list, accessed_columns_list):
    sink_def =""
    # Sink definitions
    sink_def += "# Sink definitions\n"
    qualifier = f"bench_data{num_cols}$"
    for num_cols in num_columns_list:
        sink_def += f"SINK AllSink{num_cols} TYPE Checksum"
        agg_sink = f"SINK AggSink{num_cols} TYPE Checksum UINT64 {qualifier}timestamp"
        if id_type != '':
            agg_sink += f" UINT{id_type} {qualifier}id"
        names = column_names[:num_cols]
        for col in names:
            sink_def += f" UINT64 {qualifier}{col}"
            agg_sink += f" UINT64 {qualifier}{col}"
        sink_def += "\n"
        sink_def += agg_sink + "\n"


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
        sink_def += sink + "\n"

    sink_def += "SINK MapSink TYPE Checksum UINT64 result\n\n"
    sink_def += "SINK MixedSink1 TYPE Checksum UINT64 result1\n"
    #f.write("SINK MixedSink2 TYPE Checksum UINT64 result1 UINT64 result2\n\n")
    for acc_cols in accessed_columns_list:
        if acc_cols>1:
            sink= f"SINK MixedSink{acc_cols} TYPE Checksum"
            for col_index in range(1,acc_cols + 1):#)column_names[1:acc_cols]:
                sink +=(f" UINT64 result{col_index}")
            sink_def += sink + "\n"
    sink_def +="\n"

def build_header(sources, sinks, col_names=[], docker_data_path="/tmp/nebulastream/Testing/benchmark/benchmark_results/data/benchmark_data", data_size = True):

    #single operator:

    #filter:
    #source = "bench_data{num_col}"
    # sink = "AllSink{num_col}

    #map:
    #source = f"bench_data{num_col}"
    #sink = f"MapSink{access_col-1}"
    # sink = f"MapSink{access_col}"

    #aggregation
    #source = f"bench_data{num_col}_groups{num_groups}"
    #source = f"bench_data{num_col}_groups{num_groups_idtype{id_type}"

    #sink = f"AggSink{num_col}"
    #strip bench_data prefix from file name
    source_file = sources.replace("bench_data","")
    data = "64"
    if not data_size:
        data = "8"


    source = "# Source definitions\n"
    source += f"Source {sources}"

    if "Agg" in sinks: #aggregation source
        source += f" UINT{data} timestamp"
        if 'idtype' in sources:
            id_type = sources.split('idtype')[-1]
            source += f" UINT{id_type} id"
        for col in col_names:
            source += f" UINT{data} {col}"
    else:  #filter/map source
        for col in col_names:
            source += f" UINT{data} {col}"
    if data_size:
        source += f" FILE\n{str(docker_data_path).format(source_file)}\n\n"
    else:
        source += f" FILE\n{str(docker_data_path).format(source_file+"_uint8")}\n\n"
    sink = "# Sink definitions\n"
    qualifier = f"{sources}$"
    if "All" in sinks: #filter sink
        sink += f"SINK {sinks} TYPE Checksum"
        for col in col_names:
            sink += f" UINT{data} {qualifier}{col}"
        sink += "\n"
    elif "Agg" in sinks: #agg sink
        sink += f"SINK {sinks} TYPE Checksum UINT{data} {qualifier}timestamp"
        if 'idtype' in sources:
            id_type = sources.split('idtype')[-1]
            sink += f" UINT{id_type} {qualifier}id"
        for col in col_names:
            sink += f" UINT{data} {qualifier}{col}"
        sink += "\n"
    else: #map sink
        for acc_cols in col_names:
            if acc_cols != 1:
                acc_cols -= 1
            if acc_cols < 2:
                continue
            sink += f"SINK {sinks} TYPE Checksum UINT{data} result1"
            for col_index in range(2,acc_cols + 1):#)column_names[1:acc_cols]:
                sink +=f" UINT{data} result{col_index}"
            sink += "\n"

    header = "# name: custom_benchmark.test\n"
    header += source
    header += sink


    header += "\n \n"
    return header

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark test file')
    parser.add_argument('--data', required=True, help='Path to benchmark data CSV')
    parser.add_argument('--result-dir', required=True, help='Result directory for organized structure')
    parser.add_argument('--columns', type=parse_int_list, default=[1, 2, 5, 10], help='List of number of columns to use')

    parser.add_argument('--rows', type=int, default=10000000, help='Maximum number of rows')
    parser.add_argument('--window-sizes', type=parse_int_list, default=[1000], help='Number of windows')
    parser.add_argument('--groups', type=parse_int_list, default=[100], help='Number of unique groups per column')
    parser.add_argument('--id-data-types', type=parse_str_list, default = ['', "64"], help='Data type for id column (e.g., uint32, uint64)')
    parser.add_argument('--run-options', default='all', help='options: all, single or double')
    args = parser.parse_args()

    # Customizable parameters
    params = {
        'buffer_sizes': [4000000],#40000, 400000, 4000000, 10000000, 20000000],
        'num_columns': args.columns, #, 5, 10],
        'num_rows': args.rows,
        'accessed_columns': [1, 2, 10],
        'function_types': ['add', 'mul'],
        'selectivities': [0.0001,0.1, 99, 99.9999], #[0.0001,0.1, 1, 5, 25, 50, 75, 95, 99, 99.9999],
        'agg_functions': ['count'],#'sum', 'count', 'avg', 'min', 'max'],
        'window_sizes': args.window_sizes, #[10000, 100000],
        'num_groups': args.groups, #[10, 100, 1000],
        'id_data_types':args.id_data_types, #['64'], #Data type for id column (e.g., uint32, uint64)


        'operator_chains': [
            ['map'],                  # Single map
            ['filter'],               # Single filter
            #['aggregation'],                 # Single aggregation
            ['map', 'filter'],        # Map followed by filter
            ['filter', 'map'],        # Filter followed by map
            #['filter', 'agg'],
            #['agg', 'map'],


            #['filter', 'map', 'agg'],
            #['map', 'map'],           # Two map operators
            #['filter', 'filter']      # Two filter operators
        ],
        'swap_strategy': [
            #[True, True, True],      # last swap cant be true because sink is always row
            swap_config_to_strategy([False, False, False]),    # No swaps
            #swap_config_to_strategy([False, True, False]), #TODO: implement first and second
            #swap_config_to_strategy([True, False, False]),
            swap_config_to_strategy([True, True, False]),

        ]
    }

    num_rows = args.rows

    num_windows = []
    window_sizes = params['window_sizes']
    for window_size in window_sizes:
        num_windows.append(num_rows / window_size)

    params['num_windows'] = num_windows

    #window_sizes = []
    #num_windows = params['num_windows']
    #for windows in params['num_windows']:

        #num_windows.append(num_rows / windows)
    #params['window_size'] = window_sizes


    #TODO: save params to benchmark dir
    generate_test_file(args.data, args.result_dir, params, args.run_options)

    """
    python3 src/generate_test.py --data /home/user/CLionProjects/nebulastream/Testing/benchmark/benchmark_results/data/benchmark_data_rows10000000  --result-dir /home/user/CLionProjects/nebulastream/Testing/benchmark/benchmark_results/benchmark14 --columns 1,10 --rows 10000000 --window-sizes 1000,10000,1000000 --groups 10,1000,100000 --id-data-types '',"32","64" --run-options single
    """