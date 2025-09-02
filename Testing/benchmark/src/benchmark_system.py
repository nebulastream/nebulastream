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
    parser.add_argument('--build-dir', default='cmake-build-release', help='Build directory name')
    parser.add_argument('--project-dir', default=os.environ.get('PWD', os.getcwd()), help='Project root directory')
    args = parser.parse_args()

    start_time = time.time()

    # Determine the project and build directories
    project_dir = Path(args.project_dir)
    print(f"Using project directory: {project_dir}")
    build_dir = args.build_dir

    # Create base output directory if it doesn't exist
    base_output_dir = Path(args.output_dir)
    base_output_dir.mkdir(exist_ok=True, parents=True)

    # Find the next available benchmark ID
    next_id = 1
    while (base_output_dir / f"benchmark{next_id}").exists():
        next_id += 1

    # Create the benchmark directory with incrementing ID
    benchmark_dir =  project_dir / base_output_dir / f"benchmark{next_id}"
    benchmark_dir.mkdir(exist_ok=True)
    print(f"Using benchmark directory: {benchmark_dir}")

    # Get the directory where this script is located
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    src_dir = script_dir / "src"
    if not src_dir.exists():
        src_dir = script_dir  # Fall back to script directory if src/ doesn't exist

    # Create data directory in the base output dir
    data_dir = base_output_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # Step 1: Generate data or use existing
    data_file = args.data_file
    if not args.skip_data_gen:
        print("Step 1: Generating benchmark data...")
        data_filename = f"benchmark_data_rows{args.rows}_cols{args.columns}.csv"
        data_file = data_dir / data_filename

        # Check if data with same configuration already exists
        if data_file.exists():
            print(f"Using existing data file: {data_file}")
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
        try:
            result = subprocess.run([
                "python3", str(src_dir / "generate_test.py"),
                "--data", str(data_file),
                "--output", str(test_file),
                "--result-dir", str(benchmark_dir)
            ], check=False, capture_output=True, text=True)

            if result.returncode != 0:
                print(f"Error generating test file: {result.stderr}")
                return  # Exit if test generation fails
            else:
                print("Test file generated successfully")
        except Exception as e:
            print(f"Exception when running generate_test.py: {e}")
            return

    # Step 3: Run benchmark
    print("Step 3: Running benchmarks...")
    benchmark_result_dir = subprocess.check_output([
        "python3", str(src_dir / "run_benchmark.py"),
        "--test-file", str(test_file),
        "--output-dir", str(benchmark_dir),
        "--repeats", str(args.repeats),
        "--build-dir", str(build_dir),
        "--project-dir", str(project_dir)
    ], text=True).strip()

    # Step 4: Process results
    print("Step 4: Processing benchmark results...")
    results_csv = subprocess.check_output([
        "python3", str(src_dir / "process_results.py"),
        "--benchmark-dir", benchmark_result_dir
    ], text=True).strip()

    # Step 5: Generate plots
    print("Step 5: Creating visualization plots...")
    subprocess.run([
        "python3", str(src_dir / "enhanced_plots.py"),
        "--results-csv", results_csv,
        "--output-dir", str(benchmark_dir / "charts")
    ], check=True)

    # Create a symlink to the latest benchmark directory
    latest_link = base_output_dir / "latest"
    if latest_link.exists() and latest_link.is_symlink():
        os.unlink(latest_link)
    os.symlink(benchmark_dir.name, latest_link, target_is_directory=True)

    elapsed_time = time.time() - start_time
    print(f"Benchmark completed in {elapsed_time:.2f} seconds")
    print(f"Results and visualizations available in {benchmark_dir}")
    print(f"Access the latest results at {latest_link}")

if __name__ == "__main__":
    main()