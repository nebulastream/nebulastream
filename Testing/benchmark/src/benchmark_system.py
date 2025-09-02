#!/usr/bin/env python3
import argparse
import os
import subprocess
from pathlib import Path
import time

def main():
    parser = argparse.ArgumentParser(description='NebulaBenchmark: Complete Benchmark System')
    parser.add_argument('--columns', type=int, default=10, help='Number of columns')
    parser.add_argument('--rows', type=int, default=10000000, help='Maximum number of rows')
    parser.add_argument('--repeats', type=int, default=2, help='Number of benchmark repetitions')
    parser.add_argument('--output-dir', default='benchmark_results', help='Output directory')
    parser.add_argument('--skip-data-gen', action='store_true', help='Skip data generation')
    parser.add_argument('--data-file', help='Use existing data file (with --skip-data-gen)')
    parser.add_argument('--test-file', help='Use existing test file')
    args = parser.parse_args()

    start_time = time.time()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Step 1: Generate data
    data_file = args.data_file
    if not args.skip_data_gen:
        print("Step 1: Generating benchmark data...")
        data_file = output_dir / "benchmark_data.csv"
        subprocess.run([
            "python3", "generate_data.py",
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
        test_file = output_dir / "benchmark.test"
        subprocess.run([
            "python3", "generate_test.py",
            "--data", str(data_file),
            "--output", str(test_file)
        ], check=True)

    # Step 3: Run benchmark
    print("Step 3: Running benchmarks...")
    benchmark_dir = subprocess.check_output([
        "python3", "run_benchmark.py",
        "--test-file", str(test_file),
        "--output-dir", str(output_dir),
        "--repeats", str(args.repeats)
    ], text=True).strip()

    # Step 4: Process results
    print("Step 4: Processing benchmark results...")
    results_csv = subprocess.check_output([
        "python3", "process_results.py",
        "--benchmark-dir", benchmark_dir
    ], text=True).strip()

    # Step 5: Generate plots
    print("Step 5: Creating visualization plots...")
    subprocess.run([
        "python3", "enhanced_plots.py",
        "--results-csv", results_csv
    ], check=True)

    elapsed_time = time.time() - start_time
    print(f"Benchmark completed in {elapsed_time:.2f} seconds")
    print(f"Results and visualizations available in {output_dir}")

if __name__ == "__main__":
    main()