#!/usr/bin/env python3
import os
import subprocess
import argparse
import re
import shutil
from pathlib import Path
import json

def get_config_from_file(config_file):
    config={}
    with open(config_file, 'r') as f:
        for line in f:
            name, value = line.split(':')
            config[name.strip()] = value.strip()

    return config

def run_benchmark(test_file, output_dir, repeats=2, layouts=None, build_dir="cmake-build-release", project_dir=None):
    """Run benchmark for all queries with repetitions using the new directory structure."""
    if layouts is None:
        layouts = ["ROW_LAYOUT", "COLUMNAR_LAYOUT"]

    if project_dir is None:
        project_dir = os.path.abspath(os.getcwd())

    project_dir = Path(project_dir)
    build_path = project_dir / build_dir

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
                    # Convert string representation to dict
                    config_dict = eval(config_str)
                    query_configs[query_id] = config_dict

    # Extract individual test files from the filter and map directories
    filter_dir = output_dir / "filter"
    map_dir = output_dir / "map"
    double_op_dir = output_dir / "double_operator"
    possible_dirs = [filter_dir, map_dir, double_op_dir]
    dirs = []
    if double_op_dir.exists():
        for op_dir in double_op_dir.iterdir(): #get strategy dirs inside double_op_dir
            if op_dir.is_dir():
                dirs.append(op_dir)

    for dir in possible_dirs:
        if(dir.exists()):
            dirs.append(dir)

    if (not filter_dir.exists() or not map_dir.exists()) and not double_op_dir.exists():
        print("Error: Directory structure not found. Make sure to run generate_test.py first.")
        return

    # Get all buffer size directories
    buffer_dirs = []
    for op_dir in dirs:
        for buffer_dir in op_dir.glob("bufferSize*"):
            buffer_dirs.append(buffer_dir)

    # For each buffer directory, run all queries in the test file
    for buffer_dir in buffer_dirs:
        # Find test files in this buffer directory
        test_files = list(buffer_dir.glob("*.test"))
        if not test_files:
            continue

        buffer_test_file = test_files[0]
        buffer_size = re.search(r'bufferSize(\d+)', str(buffer_dir)).group(1)

        # Get query directories for this buffer size
        query_dirs = list(buffer_dir.glob("query_*"))

        for query_dir in query_dirs:
            # Extract query ID from config file
            config_file = query_dir / "config.txt"
            if not config_file.exists():
                print(f"Warning: No config file found in {query_dir}, skipping...")
                continue
            config = get_config_from_file(config_file)

            # Read config to get query ID
            query_id = config["query_id"]

            # Read config to get test ID
            test_id = config["test_id"]
            op_chain = config["operator_chain"]
            strategy = config["swap_strategy"]

            if len(op_chain.split(",")) > 1:
                print(f"  Multiple operators in chain: {op_chain}")

                #multiple operators in query
                if strategy == "ALL_COL":
                    layouts = ["COLUMNAR_LAYOUT"]
                elif strategy == "ALL_ROW":
                    layouts = ["ROW_LAYOUT"]
                else:
                    layouts = ["ROW_LAYOUT"] #layout field gets ignored if other strategy is set
            else:
                layouts = ["ROW_LAYOUT", "COLUMNAR_LAYOUT"]

            if query_id is None:
                print(f"Warning: Could not determine query ID for {query_dir}, skipping...")
                continue

            print(f"Processing query {query_id} in {query_dir}")

            # Calculate Docker path mapping for the test file
            # The test file path must be relative to project_dir to map correctly in Docker
            docker_test_path = f"/tmp/nebulastream/Testing/benchmark/{os.path.relpath(buffer_test_file, project_dir)}"

            # Run for each layout
            for layout in layouts:
                for run in range(1, repeats + 1):
                    run_dir = query_dir
                    if len(layouts) >1:
                        run_dir = run_dir / f"run{run}_{layout}"
                    else:
                        run_dir = run_dir / f"run{run}"
                    run_dir.mkdir(exist_ok=True)

                    print(f"Running Query {query_id}, {layout}, Run {run}, BufferSize {buffer_size}...")

                    # Docker command using existing test file with query ID
                    cmd = [
                        "docker", "run", "--entrypoint=",
                        "-v", f"{os.path.expanduser('~')}/.cache/ccache:/home/ubuntu/.ccache/ccache",
                        "-v", f"/home/user/CLionProjects/nebulastream:/tmp/nebulastream",
                        "--cap-add", "SYS_ADMIN", "--cap-add", "SYS_PTRACE", "--rm",
                        "nebulastream/nes-development:local",
                        "bash", "-c", (
                            f"cd /tmp/nebulastream/cmake-build-release/nes-systests/systest/ && "
                            f"/tmp/nebulastream/cmake-build-release/nes-systests/systest/systest -b "
                            f"-t {docker_test_path}:{test_id} -- "
                            f"--worker.queryEngine.numberOfWorkerThreads=4 "
                            f"--worker.bufferSizeInBytes={buffer_size} "
                            f"--worker.numberOfBuffersInGlobalBufferManager=1000 "
                            f"--worker.defaultQueryExecution.operatorBufferSize={buffer_size} "
                            f"--worker.defaultQueryExecution.memoryLayout={layout} "
                            f"--worker.defaultQueryExecution.layoutStrategy={strategy} "
                            f"--enableGoogleEventTrace=true"
                        )
                    ]

                    try:
                        # Debug: Print Docker command
                        #print(f"  Running Docker with command pointing to test file: {docker_test_path}:{test_id}")

                        # Run the benchmark
                        result = subprocess.run(cmd, capture_output=True, text=True)

                        # Save output logs
                        with open(run_dir / "stdout.log", 'w') as f:
                            f.write(result.stdout)
                        with open(run_dir / "stderr.log", 'w') as f:
                            f.write(result.stderr)

                        if result.returncode != 0:
                            if result.stderr.strip():
                                print(f"  Error running benchmark, check logs in {run_dir}")
                                print(f"  Error output: {result.stderr[:200]}...")

                        # Copy GoogleEventTrace file if it exists
                        search_path = Path("/home/user/CLionProjects/nebulastream/cmake-build-release/nes-systests/systest")
                        trace_files = list(search_path.glob("**/GoogleEventTrace*"))
                        if trace_files:
                            # Sort by name (which includes timestamps) and take the most recent
                            trace_files.sort()
                            trace_file = trace_files[-1]
                            trace_dest = run_dir / f"GoogleEventTrace_{layout}_buffer{buffer_size}_query{query_id}.json"
                            shutil.copy(trace_file, trace_dest)
                            #print(f"  Saved most recent trace file: {trace_file.name} to {trace_dest}")
                        else:
                            print(f"  No trace file found for Query {query_id}, Run {run} in path {search_path}")

                    except Exception as e:
                        print(f"Error running benchmark: {e}", file=os.sys.stderr)

    print(f"Benchmark complete. Results in {output_dir}")
    return str(output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run benchmark tests')
    parser.add_argument('--test-file', required=True, help='Path to test file')
    parser.add_argument('--output-dir', default='benchmark_results', help='Output directory')
    parser.add_argument('--repeats', type=int, default=2, help='Number of repetitions')
    parser.add_argument('--build-dir', default='cmake-build-release', help='Build directory name')
    parser.add_argument('--project-dir', default=os.path.abspath(os.getcwd()), help='Project root directory')
    args = parser.parse_args()

    run_benchmark(
        args.test_file,
        args.output_dir,
        args.repeats,
        build_dir=args.build_dir,
        project_dir=args.project_dir
    )