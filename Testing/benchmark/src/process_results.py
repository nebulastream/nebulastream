#!/usr/bin/env python3
import os
import subprocess
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import json

def process_benchmark(benchmark_dir):
    """Process benchmark results using enginestatsread.py for each query."""
    benchmark_dir = Path(benchmark_dir)
    query_dirs = sorted([d for d in benchmark_dir.glob("Query*") if d.is_dir()])

    all_results = []

    for query_dir in query_dirs:
        query_id = int(query_dir.name.replace("Query", ""))

        # Read query parameters
        params = {}
        param_file = query_dir / "params.txt"
        if param_file.exists():
            with open(param_file, 'r') as f:
                for line in f:
                    if ':' in line:
                        key, value = line.strip().split(':', 1)
                        params[key.strip()] = value.strip()

        # Process each run directory
        run_dirs = sorted([d for d in query_dir.glob("Run*") if d.is_dir()])

        for run_dir in run_dirs:
            # Extract layout from directory name
            layout = "ROW_LAYOUT" if "ROW_LAYOUT" in run_dir.name else "COLUMNAR_LAYOUT"
            run_num = int(run_dir.name.split('_')[0].replace("Run", ""))

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
            subprocess.run(
                ["python3", "Testing/scripts/enginestatsread.py", str(temp_dir)],
                check=True
            )

            # Read CSV result from enginestatsread.py
            csv_file = temp_dir / f"temp_{query_id}_{layout}_{run_num}.csv"
            if csv_file.exists():
                df = pd.read_csv(csv_file)

                # Add query parameters and run information
                for k, v in params.items():
                    df[k] = v
                df['query_id'] = query_id
                df['run'] = run_num

                all_results.append(df)

            # Clean up temporary directory
            shutil.rmtree(temp_dir)

    # Combine all results
    if all_results:
        combined_df = pd.concat(all_results)
        output_csv = benchmark_dir / f"{benchmark_dir.name}.csv"
        combined_df.to_csv(output_csv, index=False)
        print(f"Combined results saved to {output_csv}")

        # Average results across runs
        avg_df = combined_df.groupby(['query_id', 'layout', 'buffer_size', 'operator_type']).mean().reset_index()
        avg_csv = benchmark_dir / f"{benchmark_dir.name}_avg.csv"
        avg_df.to_csv(avg_csv, index=False)
        print(f"Averaged results saved to {avg_csv}")

        return str(avg_csv)

    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process benchmark results')
    parser.add_argument('--benchmark-dir', required=True, help='Path to benchmark directory')
    args = parser.parse_args()

    process_benchmark(args.benchmark_dir)