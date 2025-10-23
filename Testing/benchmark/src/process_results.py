#!/usr/bin/env python3
import os
import subprocess
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import json
import shutil

def process_benchmark(benchmark_dir, run_options='all'): #TODO: also cover latency (especially for swaps)
    """Process benchmark results with support for double operator configurations."""
    try:
        benchmark_dir = Path(benchmark_dir)

        # Validate benchmark directory
        if not benchmark_dir.exists():
            print(f"Error: Benchmark directory does not exist: {benchmark_dir}")
            return None

        print(f"Processing results in: {benchmark_dir}")

        # Initialize directory collections based on run_options
        dirs_to_process = []

        # Setup directory structure based on run_options
        if run_options in ['single', 'all']:
            # Single operator directories
            single_op_dir = benchmark_dir
            if single_op_dir.exists():
                for op_type in ['filter', 'map', 'aggregation']:
                    op_dir = single_op_dir / op_type
                    if op_dir.exists():
                        dirs_to_process.append(op_dir)


        if run_options in ['double', 'all']:
            # Double operator directories
            double_op_dir = benchmark_dir / "double_operator"
            if double_op_dir.exists():
                for strategy_dir in double_op_dir.iterdir():
                    if strategy_dir.is_dir():
                        dirs_to_process.append(strategy_dir)

        if not dirs_to_process:
            print(f"No compatible directories found for run_options: {run_options}")
            print("Available contents:")
            for item in benchmark_dir.iterdir():
                print(f"  - {item.name}")
            return None

        # Collect all trace files and their metadata
        trace_file_info = []
        empty_files = 0

        for op_dir in dirs_to_process:
            operator_type = op_dir.name  # "filter", "map", or a swap strategy name

            # Determine if this is a single or double operator directory
            is_double_op = "double_operator" in str(op_dir)

            # Process each buffer size directory
            for buffer_dir in op_dir.glob("bufferSize*"):
                buffer_size = int(buffer_dir.name.replace("bufferSize", ""))

                query_dirs = list(buffer_dir.glob("query_*"))
                if run_options == "double" or run_options == "all":
                    for dir in buffer_dir.iterdir():
                        if dir.is_dir():
                            query_dirs.extend(dir.glob("query_*"))

                # Process each query directory
                for query_dir in query_dirs:
                    # Read query configuration
                    config = {}
                    config_file = query_dir / "config.txt"
                    if config_file.exists():
                        with open(config_file, 'r') as f:
                            for line in f:
                                if ':' in line:
                                    key, value = line.strip().split(':', 1)
                                    config[key.strip()] = value.strip()

                    query_id = int(config.get('query_id', 0))
                    if query_id == 0:
                        continue

                    # Extract operator chain info for double operators
                    operator_chain = None
                    if "operator_chain" in config:
                        operator_chain = config["operator_chain"]
                    elif "double_operator" in str(op_dir) and "_" in query_dir.name:
                        # Try to extract from directory name (query_filter_map_cols...)
                        parts = query_dir.name.split("_")
                        if len(parts) >= 3:
                            operator_chain = f"['{parts[1]}', '{parts[2]}']"

                    # Extract swap strategy
                    swap_strategy = config.get('swap_strategy', operator_type)

                    # Process each run directory
                    for run_dir in query_dir.glob("run*"):
                        # Extract run number and layout from directory name
                        run_parts = run_dir.name.split('_')
                        run_num = int(run_parts[0].replace("run", ""))
                        layout = run_parts[1] if len(run_parts) > 1 else "UNKNOWN"

                        # Find GoogleEventTrace file
                        trace_files = list(run_dir.glob("GoogleEventTrace*.json"))
                        if not trace_files:
                            continue

                        trace_file = trace_files[0]

                        if trace_file.stat().st_size == 0:
                            empty_files += 1
                            continue

                        # Store file info with metadata
                        trace_file_info.append({
                            'file_path': trace_file,
                            'query_id': query_id,
                            'run': run_num,
                            'layout': layout,
                            'buffer_size': buffer_size,
                            'operator_type': operator_type,
                            'query_dir': query_dir,
                            'config': config,
                            'is_double_op': is_double_op,
                            'operator_chain': operator_chain,
                            'swap_strategy': swap_strategy
                        })

        if not trace_file_info:
            print(f"No trace files found to process (empty ones: {empty_files})")
            return None

        print(f"Found {len(trace_file_info)} trace files to process and {empty_files} empty trace files.")

        # Create a temporary directory for enginestatsread.py output
        temp_dir = benchmark_dir / "temp_results"
        temp_dir.mkdir(exist_ok=True)

        # Get a list of all trace files
        trace_files = [str(info['file_path']) for info in trace_file_info]

        # Run enginestatsread.py with all trace files at once
        try:
            result = subprocess.run(
                ["python3", "src/enginestatsread.py",
                 str(benchmark_dir),
                 "--trace-files"] + trace_files,
                check=False, capture_output=True
            )
            if result.returncode != 0:
                print(f"Error running enginestatsread.py: {result.stderr.decode()}")
                return None
            else:
                print(result.stdout.decode())

            """
            # Read CSV result from enginestatsread.py
            csv_file = temp_dir / f"{temp_dir.name}.csv"
            if csv_file.exists():
                results_df = pd.read_csv(csv_file)

                # Process the combined results
                all_results = []

                for info in trace_file_info:
                    trace_name = info['file_path'].name
                    query_results = results_df[results_df['filename'] == trace_name].copy()

                    if not query_results.empty:
                        # Add query parameters and run information
                        for k, v in info['config'].items():
                            # Try to convert numerical values
                            try:
                                v_float = float(v)
                                query_results[k] = v_float
                            except ValueError:
                                query_results[k] = v

                        query_results['query_id'] = info['query_id']
                        query_results['run'] = info['run']
                        query_results['layout'] = info['layout']
                        query_results['buffer_size'] = info['buffer_size']
                        query_results['operator_type'] = info['operator_type']
                        query_results['is_double_op'] = info['is_double_op']
                        query_results['operator_chain'] = info['operator_chain']
                        query_results['swap_strategy'] = info['swap_strategy']

                        # For double operators, extract full query duration and calculate swap penalty
                        if info['is_double_op']:
                            # Extract durations for each operator in the pipeline
                            pipeline_stages = sorted([col for col in query_results.columns if col.startswith('pipeline_')
                                                      and '_duration' in col and not col.endswith('_avg')])

                            if pipeline_stages:
                                # Total query duration is the sum of all pipeline durations
                                query_results['full_query_duration'] = query_results[pipeline_stages].sum(axis=1)

                                # Calculate swap penalty
                                # Higher values indicate more overhead from swapping
                                swap_overhead = query_results.get('swap_overhead', 0)
                                compute_time = query_results.get('pipeline_3_compute_time', 0)
                                if not isinstance(swap_overhead, int) and not isinstance(compute_time, int):
                                    query_results['swap_penalty_ratio'] = swap_overhead / (compute_time + 1e-10)
                                    query_results['swap_penalty_ms'] = swap_overhead

                        all_results.append(query_results)

                        # Copy individual results to query directory
                        result_csv = info['query_dir'] / f"run{info['run']}_{info['layout']}_results.csv"
                        query_results.to_csv(result_csv, index=False)

                if all_results:
                    combined_df = pd.concat(all_results, ignore_index=True)

                    # Process per-query statistics
                    query_ids = combined_df['query_id'].unique()
                    for query_id in query_ids:
                        query_results = combined_df[combined_df['query_id'] == query_id]

                        # Get the query directory from first matching info
                        query_info = next((info for info in trace_file_info if info['query_id'] == query_id), None)
                        if query_info:
                            query_dir = query_info['query_dir']

                            # Calculate average results for this query across runs
                            avg_df = query_results.groupby(['layout']).mean(numeric_only=True).reset_index()
                            avg_df['operator_type'] = query_info['operator_type']
                            avg_df['is_double_op'] = query_info['is_double_op']
                            avg_df['operator_chain'] = query_info['operator_chain']
                            avg_df['swap_strategy'] = query_info['swap_strategy']

                            avg_csv = query_dir / "avg_results.csv"
                            avg_df.to_csv(avg_csv, index=False)

                    # Combine all results
                    output_csv = benchmark_dir / f"{benchmark_dir.name}_results.csv"
                    combined_df.to_csv(output_csv, index=False)

                    # Create separate results files for single and double operators
                    single_ops_df = combined_df[~combined_df['is_double_op']]
                    if not single_ops_df.empty:
                        single_csv = benchmark_dir / f"{benchmark_dir.name}_single_operator_results.csv"
                        single_ops_df.to_csv(single_csv, index=False)

                    double_ops_df = combined_df[combined_df['is_double_op']]
                    if not double_ops_df.empty:
                        double_csv = benchmark_dir / f"{benchmark_dir.name}_double_operator_results.csv"
                        double_ops_df.to_csv(double_csv, index=False)

                    # Average results across runs for all queries
                    #avg_columns = ['query_id', 'layout', 'buffer_size', 'operator_type',
                                   #'is_double_op', 'operator_chain', 'swap_strategy']
                    #avg_df = combined_df.groupby(avg_columns).mean(numeric_only=True).reset_index()

                    avg_csv = benchmark_dir / f"{benchmark_dir.name}_avg_results.csv"
                    #avg_df.to_csv(avg_csv, index=False)

                    return str(output_csv)"""

        except Exception as e:
            print(f"Error processing trace files: {e}")
            import traceback
            traceback.print_exc()

        # Clean up temporary directory
        shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"Error processing benchmark: {e}")
        import traceback
        traceback.print_exc()

    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process benchmark results')
    parser.add_argument('--benchmark-dir', required=True, help='Path to benchmark directory')
    parser.add_argument('--run-options', required=True, default='all', help='options: all, single or double')
    args = parser.parse_args()

    process_benchmark(args.benchmark_dir, args.run_options)

#python3 src/process_results.py --benchmark-dir /home/user/CLionProjects/nebulastream/Testing/benchmark/benchmark_results/benchmark14 --run-options=singl