#!/usr/bin/env python3
import argparse
import os
import subprocess
import shutil
from pathlib import Path
import time
import glob
import re

def main():
    parser = argparse.ArgumentParser(description='NebulaBenchmark: Complete Benchmark System')
    parser.add_argument('--columns', type=int, default=10, help='Number of columns')
    parser.add_argument('--rows', type=int, default=10000000, help='Maximum number of rows')
    parser.add_argument('--repeats', type=int, default=2, help='Number of benchmark repetitions')
    parser.add_argument('--output-dir', default='benchmark_results', help='Base output directory')
    parser.add_argument('--skip-data-gen', action='store_true', help='Skip data generation')
    parser.add_argument('--data-file', help='Use existing data file (with --skip-data-gen)')
    parser.add_argument('--test-file', help='Use existing test file')
    args = parser.parse_args()

    start_time = time.time()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Get the directory where this script is located
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    src_dir = script_dir

    # Create data directory
    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # Step 1: Generate data or use existing
    data_file = args.data_file
    if not args.skip_data_gen:
        print("Step 1: Generating benchmark data...")
        data_filename = f"benchmark_data_rows{args.rows}_cols{args.columns}.csv"
        data_file = data_dir / data_filename

        # Check if data with same configuration already exists
        existing_data = list(data_dir.glob(f"benchmark_data_rows{args.rows}_cols{args.columns}.csv"))
        if existing_data and existing_data[0].exists():
            print(f"Using existing data file: {existing_data[0]}")
            data_file = existing_data[0]
        else:
            subprocess.run([
                "python3", str(src_dir / "generate_data.py"),
                "--rows", str(args.rows),
                "--columns", str(args.columns),
                "--output", str(data_file)
            ], check=True)
    elif not data_file:
        print("Error: --data-file must be specified when using --skip-data-gen")
        return

    # Step 2: Generate test file
    test_file = args.test_file
    if not test_file:
        print("Step 2: Generating benchmark test file...")
        test_file = data_dir / "benchmark.test"
        subprocess.run([
            "python3", str(src_dir / "generate_test.py"),
            "--data", str(data_file),
            "--output", str(test_file),
            "--result-dir", str(output_dir)  # Pass output dir for new structure
        ], check=True)

    # Step 3: Run benchmark
    print("Step 3: Running benchmarks...")
    benchmark_dir = subprocess.check_output([
        "python3", str(src_dir / "run_benchmark.py"),
        "--test-file", str(test_file),
        "--output-dir", str(output_dir),
        "--repeats", str(args.repeats)
    ], text=True).strip()

    # Step 4: Process results
    print("Step 4: Processing benchmark results...")
    results_csv = subprocess.check_output([
        "python3", str(src_dir / "process_results.py"),
        "--benchmark-dir", benchmark_dir
    ], text=True).strip()

    # Step 5: Generate plots
    print("Step 5: Creating visualization plots...")
    subprocess.run([
        "python3", str(src_dir / "enhanced_plots.py"),
        "--results-csv", results_csv,
        "--output-dir", str(output_dir / "charts")
    ], check=True)

    elapsed_time = time.time() - start_time
    print(f"Benchmark completed in {elapsed_time:.2f} seconds")
    print(f"Results and visualizations available in {output_dir}")

if __name__ == "__main__":
    main()