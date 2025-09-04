#!/usr/bin/env python3
# cd /home/user/CLionProjects/nebulastream/Testing/benchmark
# python3 src/benchmark_system.py
import argparse
import os
import subprocess
import shutil
from pathlib import Path
import time
import glob
import re

def parse_int_list(arg):
    """Convert comma-separated string to list of integers."""
    return [int(x) for x in arg.split(',')]
def main():
    parser = argparse.ArgumentParser(description='NebulaBenchmark: Complete Benchmark System')
    parser.add_argument('--columns', type=parse_int_list, default=[1, 2, 5, 10], help='List of number of columns to use (comma-separated)')
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
        data_filename = f"benchmark_data_rows{args.rows}"
        data_file = data_dir / data_filename

        # Check if data with same configuration already exists
        if data_file.exists():
            print(f"Using existing data file: {data_file}")
        else:
            subprocess.run([
                "python3", str(src_dir / "generate_data.py"),
                "--output", str(data_file),
                "--rows", str(args.rows),
                "--columns", ','.join(map(str, args.columns))

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
                "--result-dir", str(benchmark_dir),
                "--columns", ','.join(map(str, args.columns))
            ], check=False, capture_output=True, text=True)

            if result.returncode != 0:
                print(f"Error generating test file: {result.stderr}")
                return  # Exit if test generation fails
            else:
                print(result.stdout)
                print("Test file generated successfully")
        except Exception as e:
            print(f"Exception when running generate_test.py: {e}")
            return

    # Step 3: Run benchmark
    print("Step 3: Running benchmarks...")
    process = subprocess.run([
        "python3", str(src_dir / "run_benchmark.py"),
        "--test-file", str(test_file),
        "--output-dir", str(benchmark_dir),
        "--repeats", str(args.repeats),
        "--build-dir", str(build_dir),
        "--project-dir", str(project_dir)
    ], text=True, capture_output=True, check=False)

    # Extract benchmark directory from output - look for actual paths
    benchmark_result_dir = None
    for line in process.stdout.strip().split('\n'):
        if line.startswith('/') and 'benchmark_results/benchmark' in line:
            potential_dir = line.strip()
            if Path(potential_dir).exists():
                benchmark_result_dir = potential_dir
                break

    if not benchmark_result_dir:
        print(f"Warning: Could not extract valid benchmark directory from output")
        benchmark_result_dir = str(benchmark_dir)

    print(f"Using benchmark result directory: {benchmark_result_dir}")

    # Step 4: Process results
    print("Step 4: Processing benchmark results...")
    try:
        process_result = subprocess.run([
            "python3", str(src_dir / "process_results.py"),
            "--benchmark-dir", benchmark_result_dir
        ], text=True, capture_output=True, check=False)

        print(process_result.stdout)  # Display stdout for better visibility

        # Extract the results CSV path from the output
        results_csv = None
        for line in process_result.stdout.strip().split('\n'):
            if '_avg_results.csv' in line:
                parts = line.split()
                for part in parts:
                    if part.endswith('_avg_results.csv') and Path(part).exists():
                        results_csv = part
                        break

        # If not found in output, try direct path construction
        if not results_csv or not Path(results_csv).exists():
            potential_csv = Path(benchmark_result_dir) / f"{Path(benchmark_result_dir).name}_avg_results.csv"
            if potential_csv.exists():
                results_csv = str(potential_csv)
                print(f"Found results CSV by direct lookup: {results_csv}")

        if not results_csv or not Path(results_csv).exists():
            print("Warning: Could not find results CSV")
        else:
            print(f"Using results CSV: {results_csv}")

    except Exception as e:
        print(f"Error processing results: {e}")
        import traceback
        traceback.print_exc()
        results_csv = None

    # Step 5: Generate plots
    print("Step 5: Creating visualization plots...")

    # Find enhanced_plots.py script
    script_locations = [
        src_dir / "enhanced_plots.py",
        Path("src/enhanced_plots.py"),
        Path("Testing/benchmark/src/enhanced_plots.py"),
        Path(os.path.abspath(".")) / "src" / "enhanced_plots.py"
    ]
    enhanced_plots_path = None
    for path in script_locations:
        if path.exists():
            enhanced_plots_path = path
            break

    # Find plots.py script
    plots_script_locations = [
        src_dir / "plots.py",
        Path("Testing/scripts/plots.py"),
        Path(os.path.abspath(".")) / "scripts" / "plots.py"
    ]
    plots_script_path = None
    for path in plots_script_locations:
        if path.exists():
            plots_script_path = path
            break

    if not enhanced_plots_path:
        print("Error: Could not find enhanced_plots.py script")
        print("Searched in:")
        for path in script_locations:
            print(f"  - {path}")
    else:
        try:
            # Create charts directory for global summaries
            charts_dir = Path(benchmark_result_dir) / "charts"
            charts_dir.mkdir(exist_ok=True, parents=True)

            # Find main results CSV for global charts
            main_results = Path(benchmark_result_dir) / f"{Path(benchmark_result_dir).name}_avg_results.csv"
            if main_results.exists():
                print(f"Found main results CSV: {main_results}")

                # Generate global charts with enhanced_plots
                try:
                    subprocess.run(
                        ["python3", str(enhanced_plots_path),
                         "--results-csv", str(main_results),
                         "--output-dir", str(charts_dir)],
                        check=True
                    )
                    print(f"  Success: Global enhanced plots created in {charts_dir}")
                except Exception as e:
                    print(f"  Error generating global enhanced plots: {e}")

                # Also run plots.py for the main results
                if plots_script_path:
                    pass
                    """try:
                        subprocess.run(
                            ["python3", str(plots_script_path), str(main_results)],
                            check=True
                        )
                        print(f"  Success: Additional plots created using plots.py")
                    except Exception as e:
                        print(f"  Error generating additional plots: {e}") """

            # Find individual query results for per-query plots
            query_count = 0
            for op_type in ['filter', 'map']:
                op_dir = Path(benchmark_result_dir) / op_type
                if not op_dir.exists():
                    continue

                for buffer_dir in op_dir.glob("bufferSize*"):
                    for query_dir in buffer_dir.glob("query_*"):
                        avg_csv = query_dir / "avg_results.csv"
                        if avg_csv.exists():
                            plots_dir = query_dir / "plots"
                            plots_dir.mkdir(exist_ok=True, parents=True)

                            try:
                                subprocess.run(
                                    ["python3", str(enhanced_plots_path),
                                     "--results-csv", str(avg_csv),
                                     "--output-dir", str(plots_dir)],
                                    check=True
                                )
                                query_count += 1
                            except Exception as e:
                                print(f"  Error generating plots for {avg_csv}: {e}")

            print(f"Successfully generated plots for {query_count} individual queries")

        except Exception as e:
            print(f"Error during plot generation: {e}")
            import traceback
            traceback.print_exc()

    # Create a symlink to the latest benchmark directory
    latest_link = base_output_dir / "latest"
    #if latest_link.exists() and latest_link.is_symlink():
    #    os.unlink(latest_link)
    #os.symlink(benchmark_dir.name, latest_link, target_is_directory=True)

    elapsed_time = time.time() - start_time
    print(f"Benchmark completed in {elapsed_time:.2f} seconds")
    print(f"Results and visualizations available in {benchmark_dir}")
    print(f"Access the latest results at {latest_link}")
    #todo: save stdout and shutdown system

if __name__ == "__main__":
    main()