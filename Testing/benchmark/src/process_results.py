#!/usr/bin/env python3
import os
import subprocess
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import json
import shutil

def process_benchmark(benchmark_dir, run_options='all'):
    """Process benchmark results using enginestatsread.py for each query with new directory structure."""
    try:
        benchmark_dir = Path(benchmark_dir)

        # Validate benchmark directory
        if not benchmark_dir.exists():
            print(f"Error: Benchmark directory does not exist: {benchmark_dir}")
            print(f"Raw path value received: '{benchmark_dir}'")
            return None

        if not benchmark_dir.is_dir():
            print(f"Error: Path is not a directory: {benchmark_dir}")
            return None

        print(f"Processing results in: {benchmark_dir}")

        # Find filter and map directories
        empty_files = 0
        filter_dir = benchmark_dir / "filter"
        map_dir = benchmark_dir / "map"

        # Check if expected subdirectories exist
        if not filter_dir.exists() and not map_dir.exists():
            print(f"Warning: Neither 'filter' nor 'map' directories found in {benchmark_dir}")
            print("Available contents:")
            for item in benchmark_dir.iterdir():
                print(f"  - {item.name}")
            return None

        # Collect all trace files and their metadata
        trace_file_info = []

        for op_dir in [filter_dir, map_dir]:
            if not op_dir.exists():
                continue

            operator_type = op_dir.name  # "filter" or "map"
            #print(f"Collecting trace files for {operator_type} operations...")

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

                        if trace_file.stat().st_size == 0:
                            empty_files += 1
                            #print(f"Warning: Empty trace file found: {trace_file}, skipping...")
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
                            'config': config
                        })

        if not trace_file_info:
            print(f"No trace files found to process (emtpy ones: {empty_files})")
            return None

        print(f"Found {len(trace_file_info)} trace files to process and {empty_files} empty trace files.")

        # Create a temporary directory for enginestatsread.py output
        temp_dir = benchmark_dir / "temp_results"
        temp_dir.mkdir(exist_ok=True)

        # Get a list of all trace files
        trace_files = [str(info['file_path']) for info in trace_file_info]

        # Run enginestatsread.py with all trace files at once
        try:
            subprocess.run(
                ["python3", "src/enginestatsread.py",
                 str(temp_dir),
                 "--trace-files"] + trace_files,
                check=True
            )

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
                            avg_csv = query_dir / "avg_results.csv"
                            avg_df.to_csv(avg_csv, index=False)


                    # Combine all results
                    output_csv = benchmark_dir / f"{benchmark_dir.name}_results.csv"
                    combined_df.to_csv(output_csv, index=False)
                    #print(f"Combined results saved to {output_csv}")

                    # Average results across runs for all queries
                    avg_df = combined_df.groupby(['query_id', 'layout', 'buffer_size', 'operator_type']).mean(numeric_only=True).reset_index()

                    avg_csv = benchmark_dir / f"{benchmark_dir.name}_avg_results.csv"
                    avg_df.to_csv(avg_csv, index=False)
                    #print(f"Averaged results saved to {avg_csv}")

                    return str(avg_csv)

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
    parser.add_argument('--run-options', required=True, default='double', help='options: all, single or double')
    args = parser.parse_args()

    process_benchmark(args.benchmark_dir, args.run_options)