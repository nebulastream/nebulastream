#!/usr/bin/env python3
import os
import subprocess
import argparse
import re
import shutil
from pathlib import Path
import json

def run_benchmark(test_file, output_dir, repeats=2, layouts=None):
    """Run benchmark for all queries with repetitions using the new directory structure."""
    if layouts is None:
        layouts = ["ROW_LAYOUT", "COLUMNAR_LAYOUT"]

    # Create benchmark directory structure
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Find test file path
    test_file = Path(test_file)

    # Read the query mapping file to get query configurations
    query_mapping_file = output_dir / "query_mapping.txt"
    query_configs = {}
    if query_mapping_file.exists():
        with open(query_mapping_file, 'r') as f:
            for line in f:
                if line.startswith("Query "):
                    parts = line.split(":", 1)
                    query_id = int(parts[0].replace("Query ", "").strip())
                    config_str = parts[1].strip()
                    # Convert string representation to dict (simplified approach)
                    config_dict = eval(config_str)
                    query_configs[query_id] = config_dict

    # Extract individual test files from the filter and map directories
    filter_dir = output_dir / "filter"
    map_dir = output_dir / "map"

    if not filter_dir.exists() or not map_dir.exists():
        print("Error: Directory structure not found. Make sure to run generate_test.py first.")
        return

    # Get all buffer size directories
    buffer_dirs = []
    for op_dir in [filter_dir, map_dir]:
        for buffer_dir in op_dir.glob("bufferSize*"):
            buffer_dirs.append(buffer_dir)

    PROJECT_DIR = os.path.abspath(os.getcwd())

    # For each buffer directory, run all queries in the test file
    for buffer_dir in buffer_dirs:
        # Find test files in this buffer directory
        test_files = list(buffer_dir.glob("*.test"))
        if not test_files:
            continue

        buffer_test_file = test_files[0]
        buffer_size = re.search(r'bufferSize(\d+)', str(buffer_dir)).group(1)
        operator_type = "filter" if "filter" in str(buffer_dir) else "map"

        # Extract queries from test file
        with open(buffer_test_file, 'r') as f:
            content = f.read()

        # Split file by queries (sections starting with "# Query")
        query_sections = re.split(r'# Query \d+:', content)

        # First section is the header with source/sink definitions
        header = query_sections[0]

        # Process each query section
        for i, section in enumerate(query_sections[1:], 1):
            query_text = f"# Query {i}:" + section

            # Find the query ID from the text
            query_id_match = re.search(r'# Query (\d+):', query_text)
            if not query_id_match:
                continue

            query_id = int(query_id_match.group(1))

            # Find the query directory based on config
            config = query_configs.get(query_id, {})
            if not config:
                # Try to extract parameters from query text
                params = {}
                buffer_match = re.search(r'BufferSize: (\d+)', query_text)
                if buffer_match:
                    params['buffer_size'] = buffer_match.group(1)

                op_match = re.search(r'OperatorType: (\w+)', query_text)
                if op_match:
                    params['operator_type'] = op_match.group(1)

                if params.get('operator_type') == 'filter':
                    sel_match = re.search(r'Selectivity: (\d+)', query_text)
                    if sel_match:
                        params['selectivity'] = sel_match.group(1)

                    cols_match = re.search(r'NumColumns: (\d+)', query_text)
                    if cols_match:
                        params['num_columns'] = cols_match.group(1)

                    access_match = re.search(r'AccessedColumns: (\d+)', query_text)
                    if access_match:
                        params['accessed_columns'] = access_match.group(1)

                    query_dir = buffer_dir / f"query_sel{params.get('selectivity', 'unknown')}_cols{params.get('num_columns', 'unknown')}_access{params.get('accessed_columns', 'unknown')}"

                elif params.get('operator_type') == 'map':
                    func_match = re.search(r'FunctionType: (\w+)', query_text)
                    if func_match:
                        params['function_type'] = func_match.group(1)

                    cols_match = re.search(r'NumColumns: (\d+)', query_text)
                    if cols_match:
                        params['num_columns'] = cols_match.group(1)

                    access_match = re.search(r'AccessedColumns: (\d+)', query_text)
                    if access_match:
                        params['accessed_columns'] = access_match.group(1)

                    query_dir = buffer_dir / f"query_{params.get('function_type', 'unknown')}_cols{params.get('num_columns', 'unknown')}_access{params.get('accessed_columns', 'unknown')}"

                else:
                    # Skip if we can't determine the query directory
                    continue
            else:
                # Use the config from the mapping file
                if config['operator_type'] == 'filter':
                    query_dir = buffer_dir / f"query_sel{config['selectivity']}_cols{config['num_columns']}_access{config['accessed_columns']}"
                else:
                    query_dir = buffer_dir / f"query_{config['function_type']}_cols{config['num_columns']}_access{config['accessed_columns']}"

            # Ensure query directory exists
            if not query_dir.exists():
                query_dir.mkdir(parents=True, exist_ok=True)

            # Save config if not already saved
            if not (query_dir / "config.txt").exists():
                with open(query_dir / "config.txt", 'w') as f:
                    for k, v in config.items():
                        f.write(f"{k}: {v}\n")

            # Create a temporary test file with just this query
            temp_test_file = query_dir / "query.test"
            with open(temp_test_file, 'w') as f:
                f.write(header)
                f.write(query_text)

            # Run for each layout
            for layout in layouts:
                for run in range(1, repeats + 1):
                    run_dir = query_dir / f"run{run}_{layout}"
                    run_dir.mkdir(exist_ok=True)

                    print(f"Running Query {query_id}, {layout}, Run {run}, BufferSize {buffer_size}...")

                    # Docker command similar to benchmark.sh
                    cmd = [
                        "docker", "run", "--entrypoint=",
                        "-v", f"/home/user/.cache/ccache:/home/ubuntu/.ccache/ccache",
                        "-v", f"{PROJECT_DIR}:/tmp/nebulastream",
                        "--cap-add", "SYS_ADMIN", "--cap-add", "SYS_PTRACE", "--rm",
                        "nebulastream/nes-development:local",
                        "bash", "-c", (
                            f"cd /tmp/nebulastream/cmake-build-release/nes-systests/systest/ && "
                            f"/tmp/nebulastream/cmake-build-release/nes-systests/systest/systest -b "
                            f"-t {os.path.abspath(temp_test_file)}:1 -- "
                            f"--worker.queryEngine.numberOfWorkerThreads=4 "
                            f"--worker.bufferSizeInBytes={buffer_size} "
                            f"--worker.numberOfBuffersInGlobalBufferManager=1000 "
                            f"--worker.defaultQueryExecution.operatorBufferSize={buffer_size} "
                            f"--worker.defaultQueryExecution.memoryLayout={layout} "
                            f"--worker.defaultQueryExecution.layoutStrategy=USE_SINGLE_LAYOUT "
                            f"--enableGoogleEventTrace=true"
                        )
                    ]

                    try:
                        # Run the benchmark
                        result = subprocess.run(cmd, capture_output=True, text=True)

                        # Save output logs
                        with open(run_dir / "stdout.log", 'w') as f:
                            f.write(result.stdout)
                        with open(run_dir / "stderr.log", 'w') as f:
                            f.write(result.stderr)

                        # Copy GoogleEventTrace file if it exists
                        trace_files = list(Path(f"{PROJECT_DIR}/cmake-build-release/nes-systests").glob("GoogleEventTrace*"))
                        if trace_files:
                            trace_file = trace_files[0]
                            trace_dest = run_dir / f"GoogleEventTrace_{layout}_buffer{buffer_size}_query{query_id}.json"
                            shutil.copy(trace_file, trace_dest)
                            print(f"  Saved trace to {trace_dest}")
                        else:
                            print(f"  No trace file found for Query {query_id}, Run {run}")

                    except Exception as e:
                        print(f"Error running benchmark: {e}")

    print(f"Benchmark complete. Results in {output_dir}")
    return str(output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run benchmark tests')
    parser.add_argument('--test-file', required=True, help='Path to test file')
    parser.add_argument('--output-dir', default='benchmark_results', help='Output directory')
    parser.add_argument('--repeats', type=int, default=2, help='Number of repetitions')
    args = parser.parse_args()

    run_benchmark(args.test_file, args.output_dir, args.repeats)