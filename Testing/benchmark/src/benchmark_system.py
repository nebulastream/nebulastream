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
from utils import parse_int_list, parse_str_list
import pandas as pd

def find_latest_benchmark_dir(base_dir):
    """Find the most recent benchmark directory."""
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return None

    benchmark_dirs = list(base_dir.glob("benchmark*"))
    if not benchmark_dirs:
        return None

    # Extract benchmark numbers and find max
    benchmark_numbers = []
    for dir_path in benchmark_dirs:
        match = re.search(r"benchmark(\d+)", str(dir_path))
        if match:
            benchmark_numbers.append(int(match.group(1)))

    if not benchmark_numbers:
        return None

    latest_id = max(benchmark_numbers)
    return base_dir / f"benchmark{latest_id}"

def main():
    parser = argparse.ArgumentParser(description='NebulaBenchmark: Complete Benchmark System')
    parser.add_argument('--columns', type=parse_int_list, default=[1,2,10,20,50], help='List of number of columns to use (comma-separated)') #TODO more than 1k cols -> run seperately
    parser.add_argument('--window-sizes', type=parse_int_list, default=[10000, 1000000], help='Number of windows')
    parser.add_argument('--groups', type=parse_int_list, default=[10, 100000], help='Number of unique groups per column')
    parser.add_argument('--rows', type=int, default=1000000, help='Maximum number of rows')
    parser.add_argument('--repeats', type=int, default=2, help='Number of benchmark repetitions')
    parser.add_argument('--id-data-types', type=parse_str_list, default = ['', "16", "64"], help='Data type for id column (e.g., uint32, uint64), empty for no id column')
    parser.add_argument('--threads', type=parse_int_list, default=[1, 4], help='Number of worker threads to use')
    parser.add_argument('--output-dir', default='benchmark_results', help='Base output directory')
    parser.add_argument('--skip-data-gen', action='store_true', help='Skip data generation')
    parser.add_argument('--skip-test-gen', action='store_true', help='Skip test generation')
    parser.add_argument('--skip-benchmark', action='store_true', help='Skip running benchmarks')
    parser.add_argument('--skip-processing', action='store_true', help='Skip processing results')
    parser.add_argument('--skip-plotting', action='store_true', help='Skip plotting')
    parser.add_argument('--data-file', help='Use existing data file (with --skip-data-gen)')
    parser.add_argument('--test-file', help='Use existing test file')
    parser.add_argument('--build-dir', default='cmake-build-release', help='Build directory name')
    parser.add_argument('--project-dir', default=os.environ.get('PWD', os.getcwd()), help='Project root directory')
    parser.add_argument('--run-options', default='single', help='options: all, single or double')
    parser.add_argument('--use-latest', action='store_true', help='Use latest benchmark directory instead of creating new one')
    args = parser.parse_args()#TODO: store output into log file

    start_time = time.time()

    # Determine the project and build directories
    project_dir = Path(args.project_dir)
    print(f"Using project directory: {project_dir}")
    build_dir = args.build_dir

    # Create base output directory if it doesn't exist
    base_output_dir = Path(args.output_dir)
    base_output_dir.mkdir(exist_ok=True, parents=True)

    #TODO: fix If both --skip-data-gen is true and no --data-file is provided, the code might fail without clear error handling.

    # Determine which benchmark directory to use
    benchmark_dir = None
    if args.use_latest:
        benchmark_dir = find_latest_benchmark_dir(base_output_dir)
        if benchmark_dir:
            print(f"Using latest benchmark directory: {benchmark_dir}")
        else:
            print("No existing benchmark directories found. Creating new one.")

    if not benchmark_dir:
        # Create the benchmark directory with incrementing ID
        next_id = 1
        while (base_output_dir / f"benchmark{next_id}").exists():
            next_id += 1
        benchmark_dir =  project_dir / base_output_dir / f"benchmark{next_id}"
        benchmark_dir.mkdir(exist_ok=True)
        print(f"Created new benchmark directory: {benchmark_dir}")

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
    """
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
                "--columns", ','.join(map(str, args.columns)),
                "--groups", ','.join(map(str, args.groups)),
                "--id-data-types", ','.join(args.id_data_types)

            ], check=True)
    #elif not data_file:
        #print("Error: --data-file must be specified when using --skip-data-gen")
       # return
    """
    # Step 2: Generate test file
    test_file = args.test_file
    data_base_name = f"benchmark_data_rows{args.rows}_cols{args.columns}.csv"
    if not args.skip_test_gen:
        print("Step 2: Generating benchmark test file...")
        test_file = data_dir / "benchmark.test"
        try:
            result = subprocess.run([
                "python3", str(src_dir / "generate_test.py"),
                "--data", str(data_base_name),
                "--result-dir", str(benchmark_dir),
                "--columns", ','.join(map(str, args.columns)),
                "--rows", str(args.rows),
                "--window-sizes", ','.join(map(str, args.window_sizes)),
                "--groups", ','.join(map(str, args.groups)),
                "--id-data-types", ','.join(args.id_data_types),
                "--run-options", args.run_options
            ], check=False, capture_output=True, text=True)

            if result.returncode != 0:
                print(f"Error generating test file: {result.stderr}")
                return
            else:
                print(result.stdout)
                print("Test file generated successfully")
        except Exception as e:
            print(f"Exception when running generate_test.py: {e}")
            return
    elif not test_file:
        # Find test files in benchmark directory or data directory
        test_files = list((benchmark_dir / "data").glob("*.test"))
        if not test_files:
            test_files = list(data_dir.glob("*.test"))

        if test_files:
            test_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            test_file = test_files[0]
            print(f"Using most recent test file: {test_file}")
        else:
            print("Error: No test file specified and none found in data or benchmark directory")
            return

    #TODO: let run benchmark print in real time

    # Step 3: Run benchmark
    if not args.skip_benchmark:
        print(f"Step 3: Running benchmarks with {args.repeats} repetitions...")
        try:
            result = subprocess.run([
                "python3", str(src_dir / "run_benchmark.py"),
                "--test-file", str(test_file),
                "--output-dir", str(benchmark_dir),
                "--repeats", str(args.repeats),
                "--build-dir", str(build_dir),
                "--project-dir", str(project_dir),
                "--run-options", args.run_options,
                "--threads", ','.join(map(str, args.threads))
            ], text=True, capture_output=True, check=False)

            if result.returncode != 0:
                print(f"Error running tests: {result.stderr}")
                print(f"flushing stdout: {result.stdout}")
                return
            else:
                print(result.stdout)
                print("Tests ran successfully")
        except Exception as e:
            print(f"Exception when running run_benchmark.py: {e}")
            return

    # Extract benchmark directory from output if needed
    benchmark_result_dir = str(benchmark_dir)

    print(f"Using benchmark result directory: {benchmark_result_dir}")
    """
    # Step 4: Process results
    if not args.skip_processing:
        print("Step 4: Processing benchmark results...")
        try:
            process_result = subprocess.run([
                "python3", str(src_dir / "process_results.py"),
                "--benchmark-dir", benchmark_result_dir,
                "--run-options", args.run_options
            ], text=True, capture_output=True, check=False)

            if process_result.returncode != 0:
                print(f"Error processing benchmarks: {process_result.stderr}")
                return
            else:
                print(process_result.stdout)
                print("processed trace files successfully")

            # Extract the results CSV path from the output
            results_csv = None
            for line in process_result.stdout.strip().split('\n'):
                if '_results.csv' in line:
                    parts = line.split()
                    for part in parts:
                        if part.endswith('_results.csv') and Path(part).exists():
                            results_csv = part
                            break

            # If not found in output, try direct path construction
            if not results_csv or not Path(results_csv).exists():
                potential_csv = Path(benchmark_result_dir) / f"{Path(benchmark_result_dir).name}_results.csv"
                if potential_csv.exists():
                    results_csv = str(potential_csv)
                    print(f"Found results CSV by direct lookup: {results_csv}")

        except Exception as e:
            print(f"Error processing results: {e}")
            import traceback
            traceback.print_exc()
            results_csv = None
    """
    # Step 5: Generate plots
    if not args.skip_plotting:
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

        if not enhanced_plots_path:
            print("Error: Could not find enhanced_plots.py script")
        else:
            try:
                # Create charts directory for global summaries
                charts_dir = Path(benchmark_result_dir) / "charts"
                charts_dir.mkdir(exist_ok=True, parents=True)

                # Find main results CSV for global charts
                main_results = Path(benchmark_result_dir) / f"{Path(benchmark_result_dir).name}.csv"
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
                main_results = Path(benchmark_result_dir) / f"{Path(benchmark_result_dir).name}_old.csv"
                charts_dir = Path(benchmark_result_dir) / "charts_old"
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
                """
                # Process individual query results (single operator)
                query_count = 0
                single_op_dir = Path(benchmark_result_dir) / "single_operator"
                if single_op_dir.exists():
                    for op_type in ['filter', 'map']:
                        op_dir = single_op_dir / op_type
                        if not op_dir.exists():
                            continue

                        for buffer_dir in op_dir.glob("bufferSize*"):
                            for query_dir in buffer_dir.glob("query_*"):
                                avg_csv = query_dir / "results.csv"
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
                # Process double operator results
                double_op_dir = Path(benchmark_result_dir) / "double_operator"
                if double_op_dir.exists():
                    try:
                        # First create a consolidated CSV for all double operator results
                        double_op_results = Path(benchmark_result_dir) / f"{Path(benchmark_result_dir).name}_double_operator_results.csv"

                        if double_op_results.exists():
                            # Create plots directory
                            double_op_plots_dir = double_op_dir / "plots"
                            double_op_plots_dir.mkdir(exist_ok=True, parents=True)

                            # Generate plots using enhanced_plots.py
                            subprocess.run(
                                ["python3", str(enhanced_plots_path),
                                 "--results-csv", str(double_op_results),
                                 "--output-dir", str(double_op_plots_dir)],
                                check=True
                            )
                            print(f"  Success: Double operator plots created in {double_op_plots_dir}")

                            # Also generate operator chain specific plots
                            for chain_dir in double_op_dir.glob("bufferSize*/*/"):
                                if chain_dir.is_dir():
                                    chain_name = chain_dir.name
                                    chain_csv = double_op_dir / f"{chain_name}_results.csv"

                                    # If we don't have a chain-specific CSV, we need to generate it
                                    if not chain_csv.exists():
                                        # Find query results for this chain
                                        chain_data = []
                                        for query_dir in chain_dir.glob("query_*"):
                                            avg_results = query_dir / "results.csv"
                                            if avg_results.exists():
                                                chain_data.append(pd.read_csv(avg_results))

                                        if chain_data:
                                            chain_df = pd.concat(chain_data, ignore_index=True)
                                            chain_df.to_csv(chain_csv, index=False)

                                    if chain_csv.exists():
                                        chain_plots_dir = double_op_dir / f"plots_{chain_name}"
                                        chain_plots_dir.mkdir(exist_ok=True, parents=True)

                                        try:
                                            subprocess.run(
                                                ["python3", str(enhanced_plots_path),
                                                 "--results-csv", str(chain_csv),
                                                 "--output-dir", str(chain_plots_dir)],
                                                check=True
                                            )
                                            print(f"  Success: Created plots for {chain_name} in {chain_plots_dir}")
                                        except Exception as e:
                                            print(f"  Error generating plots for {chain_name}: {e}")

                    except Exception as e:
                        print(f"  Error generating double operator plots: {e}")
                """


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

if __name__ == "__main__":
    main()

#pplot latest dir:
#python3 src/benchmark_system.py   --skip-data-gen   --skip-test-gen   --skip-benchmark   --skip-processing   --output-dir benchmark_results   --use-latest