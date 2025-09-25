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

def run_benchmark(test_file, output_dir, repeats=2, run_options="all", layouts=None, build_dir="cmake-build-release", project_dir=None):
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

    # Determine which directories to process based on run_options
    filter_dir = output_dir / "filter"
    map_dir = output_dir / "map"
    agg_dir = output_dir / "aggregation"
    double_op_dir = output_dir / "double_operator"

    dirs_to_process = []

    # Add appropriate directories based on run_options
    if run_options == "all" or run_options == "single":
        if filter_dir.exists():
            dirs_to_process.append(filter_dir)
        if map_dir.exists():
            dirs_to_process.append(map_dir)
        if agg_dir.exists():
            dirs_to_process.append(agg_dir)

    if run_options == "all" or run_options == "double":
        if double_op_dir.exists():
            # For double operators, process each strategy directory
            for strategy_dir in double_op_dir.iterdir():
                if strategy_dir.is_dir():
                    dirs_to_process.append(strategy_dir)

    if not dirs_to_process:
        print(f"Error: No directories found for run_option '{run_options}'")
        return

    # Get all buffer size directories from selected directories
    buffer_dirs = []
    for op_dir in dirs_to_process:
        for buffer_dir in op_dir.glob("bufferSize*"):
            buffer_dirs.append(buffer_dir)

    # For each buffer directory, run all queries in the test files
    for buffer_dir in buffer_dirs:
        # Find test files in this buffer directory
        test_files = list(buffer_dir.glob("*.test"))
        if not test_files:
            print(f"Warning: No test files found in {buffer_dir}, skipping...")
            continue

        # Process all test files in the directory
        for buffer_test_file in test_files:
            print(f"Processing test file: {buffer_test_file}")
            buffer_size = re.search(r'bufferSize(\d+)', str(buffer_dir)).group(1)
            #available_memory = 201,830,776,832//2  # Use half of 200GB for buffer allocation
            #requiredMemory= buffer_size * numberOfBuffers
            #numberOfBuffers = int(available_memory) // int(buffer_size)
            numberOfBuffers = 1000 #TODO check for what needed when multiple operators
            if 'agg' in buffer_test_file.name:
                numberOfBuffers = 20000
            # Extract strategy from parent directory name for double operators
            parent_dir = buffer_dir.parent
            strategy = parent_dir.name if parent_dir.name in ["ALL_ROW", "ALL_COL", "FIRST", "SECOND"] else "USE_SINGLE_LAYOUT"

            # Get query directories for this buffer size

            query_dirs = []

            if run_options == "all" or run_options == "single": #TODO: use util func for query dir extraction
                query_dirs.extend(list(buffer_dir.glob("query_*")))
                if run_options == "all":
                    for dir in buffer_dir.iterdir():
                        if dir.is_dir():
                            query_dirs.extend(list(dir.glob("query_*")))
            else:

                for dir in buffer_dir.iterdir():
                    if dir.is_dir():
                        #print(f"running queries from subdirectory: {dir}")
                        query_dirs.extend(list(dir.glob("query_*")))

            for query_dir in query_dirs:
                # Extract query ID from config file
                config_file = query_dir / "config.txt"
                if not config_file.exists():
                    print(f"Warning: No config file found in {query_dir}, skipping...")
                    continue
                config = get_config_from_file(config_file)

                # Read config to get query ID and test ID
                query_id = config["query_id"]
                test_id = config["test_id"]
                op_chain = config["operator_chain"]

                # Get strategy from config or use extracted one
                config_strategy = config.get("swap_strategy", strategy)

                # Determine which layouts to use based on strategy and operator chain
                if "," in op_chain or config_strategy == "ALL_COL":
                    layouts = ["COLUMNAR_LAYOUT"] if config_strategy == "ALL_COL" else ["ROW_LAYOUT"]
                else:
                    layouts = ["ROW_LAYOUT", "COLUMNAR_LAYOUT"]

                # Run for each layout
                for layout in layouts:
                    for run in range(1, repeats + 1):
                        run_dir = query_dir
                        if len(layouts) > 1:
                            run_dir = run_dir / f"run{run}_{layout}"
                        else:
                            run_dir = run_dir / f"run{run}"
                        run_dir.mkdir(exist_ok=True)

                        # Calculate Docker path mapping for the test file
                        docker_test_path = f"/tmp/nebulastream/Testing/benchmark/{os.path.relpath(buffer_test_file, project_dir)}"
                        #print (f"Using test file in Docker: {docker_test_path}")
                        #print(f"Running Query {query_id}, {layout}, Run {run}, BufferSize {buffer_size}, layout {layout}, strat {config_strategy}...")
                        
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
                                f"--worker.numberOfBuffersInGlobalBufferManager={numberOfBuffers} "
                                f"--worker.defaultQueryExecution.operatorBufferSize={buffer_size} "
                                f"--worker.defaultQueryExecution.memoryLayout={layout} "
                                f"--worker.defaultQueryExecution.layoutStrategy={config_strategy} "
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

                            if result.returncode != 0:
                                if result.stderr.strip():
                                    print(f"  Error running benchmark, check logs in {run_dir}")
                                    print(f"  Error output: {result.stderr[:200]}...")

                            # Copy GoogleEventTrace file if it exists
                            search_path = Path("/home/user/CLionProjects/nebulastream/cmake-build-release/nes-systests/systest")
                            trace_files = list(search_path.glob("**/GoogleEventTrace*"))
                            if trace_files:
                                trace_files.sort()
                                trace_file = trace_files[-1]
                                trace_dest = run_dir / f"GoogleEventTrace_{layout}_buffer{buffer_size}_query{query_id}.json"
                                shutil.copy(trace_file, trace_dest)
                                #print(f"  Saved most recent trace file: {trace_file.name} to {trace_dest}")
                            else:
                                print(f"  No trace file found for Query {query_id}, Run {run}")
                            search_path = Path("/home/user/CLionProjects/nebulastream/cmake-build-release/nes-systests")
                            log_files = list(search_path.glob("**/*.log"))
                            if log_files:
                                log_files.sort()
                                log_file = log_files[-1]
                                log_dest = run_dir / f"{log_file.name.replace('.log', '')}_{layout}_buffer{buffer_size}_query{query_id}.log"
                                shutil.copy(log_file, log_dest)
                                #print(f"  Saved most recent log file: {log_file.name} to {log_dest}")
                            else:
                                print(f"  No log file found for Query {query_id}, Run {run}")

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
    parser.add_argument('--run-options', default='all', help='options: all, single or double')
    args = parser.parse_args()

    run_benchmark(
        args.test_file,
        args.output_dir,
        args.repeats,
        args.run_options,
        build_dir=args.build_dir,
        project_dir=args.project_dir

    )