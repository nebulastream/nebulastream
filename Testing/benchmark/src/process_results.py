#!/usr/bin/env python3
import os
import subprocess
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import json
import shutil

def process_benchmark(benchmark_dir):
    """Process benchmark results using enginestatsread.py for each query with new directory structure."""
    benchmark_dir = Path(benchmark_dir)

    # Find filter and map directories
    filter_dir = benchmark_dir / "filter"
    map_dir = benchmark_dir / "map"

    all_results = []

    # Process each operator type directory
    for op_dir in [filter_dir, map_dir]:
        if not op_dir.exists():
            continue

        operator_type = op_dir.name  # "filter" or "map"

        # Process each buffer size directory
        for buffer_dir in op_dir.glob("bufferSize*"):
            buffer_size = int(buffer_dir.name.replace("bufferSize", ""))

            # Process each query directory
            for query_dir in buffer_dir.glob("query_*"):
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

                    # Create a temporary directory for enginestatsread.py
                    temp_dir = benchmark_dir / f"temp_{query_id}_{layout}_{run_num}"
                    temp_dir.mkdir(exist_ok=True)
                    logs_dir = temp_dir / "logs"
                    logs_dir.mkdir(exist_ok=True)

                    # Copy trace file to the logs directory
                    trace_dest = logs_dir / trace_file.name
                    shutil.copy(trace_file, trace_dest)

                    # Run enginestatsread.py
                    try:
                        subprocess.run(
                            ["python3", "Testing/scripts/enginestatsread.py", str(temp_dir)],
                            check=True
                        )

                        # Read CSV result from enginestatsread.py
                        csv_file = temp_dir / f"temp_{query_id}_{layout}_{run_num}.csv"
                        if csv_file.exists():
                            df = pd.read_csv(csv_file)

                            # Add query parameters and run information
                            for k, v in config.items():
                                # Try to convert numerical values
                                try:
                                    v_float = float(v)
                                    df[k] = v_float
                                except ValueError:
                                    df[k] = v

                            df['query_id'] = query_id
                            df['run'] = run_num
                            df['layout'] = layout
                            df['buffer_size'] = buffer_size
                            df['operator_type'] = operator_type

                            all_results.append(df)

                            # Copy the CSV to the query directory for reference
                            shutil.copy(csv_file, query_dir / f"run{run_num}_{layout}_results.csv")

                    except Exception as e:
                        print(f"Error processing trace for query {query_id}, run {run_num}: {e}")

                    # Clean up temporary directory
                    shutil.rmtree(temp_dir)

                # Calculate average results for this query across runs
                if all_results:
                    # Filter results for just this query
                    query_results = [df for df in all_results if df['query_id'].iloc[0] == query_id]
                    if query_results:
                        query_df = pd.concat(query_results)
                        avg_df = query_df.groupby(['layout']).mean().reset_index()
                        avg_csv = query_dir / "avg_results.csv"
                        avg_df.to_csv(avg_csv, index=False)

                        # Generate plots for this query
                        try:
                            subprocess.run([
                                "python3", "Testing/benchmark/src/enhanced_plots.py",
                                "--results-csv", str(avg_csv),
                                "--output-dir", str(query_dir / "plots")
                            ], check=True)
                        except Exception as e:
                            print(f"Error generating plots for query {query_id}: {e}")

    # Combine all results
    if all_results:
        combined_df = pd.concat(all_results)
        output_csv = benchmark_dir / f"{benchmark_dir.name}_results.csv"
        combined_df.to_csv(output_csv, index=False)
        print(f"Combined results saved to {output_csv}")

        # Average results across runs for all queries
        avg_df = combined_df.groupby(['query_id', 'layout', 'buffer_size', 'operator_type']).mean().reset_index()
        avg_csv = benchmark_dir / f"{benchmark_dir.name}_avg_results.csv"
        avg_df.to_csv(avg_csv, index=False)
        print(f"Averaged results saved to {avg_csv}")

        return str(avg_csv)

    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process benchmark results')
    parser.add_argument('--benchmark-dir', required=True, help='Path to benchmark directory')
    args = parser.parse_args()

    process_benchmark(args.benchmark_dir)