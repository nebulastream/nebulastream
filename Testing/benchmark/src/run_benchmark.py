#!/usr/bin/env python3
import os
import subprocess
import argparse
import re
import shutil
from pathlib import Path
import json
import csv
import pandas as pd
import concurrent
import multiprocessing
import concurrent.futures
from collections import defaultdict
from benchmark_system import parse_int_list

def get_config_from_file(config_file):
    # Read the query mapping file to get query configurations
    if config_file.suffix == '.txt':
        config={}
        with open(config_file, 'r') as f:
            for line in f:
                name, value = line.split(':')
                config[name.strip()] = value.strip()

        return config

    #else assume csv
    query_configs = []
    if config_file.exists():
        query_configs = pd.read_csv(config_file, index_col='query_id').to_dict(orient='index')
        #{'test_id': 1, 'buffer_size': 4000, 'num_columns': 1, 'accessed_columns': 1, 'operator_chain': "['filter']", 'swap_strategy': 'USE_SINGLE_LAYOUT', 'selectivity': 5.0, 'individual_selectivity': 5.0, 'threshold': 4080218930.0, 'function_type': nan, 'aggregation_function': nan, 'window_size': nan, 'num_groups': nan, 'id_data_type': nan}
        if config_file.suffix == '.csvo': # old version
            config=defaultdict(dict)
            columns= []
            for col in range(len(query_configs)):
                columns.append(query_configs[col]['query_id'])
            for query_id in range(len(query_configs[1])):
                for col in range(len(columns)):
                    config[query_id][col] = query_configs[col]['query_id']
            return config

    else:
        #raise error if file does not exist
        raise FileNotFoundError(f"Config file {config_file} does not exist.")


    return query_configs

def json_to_csv(trace_file):
    output_csv = trace_file.with_suffix('.csv')

    """Convert a JSON trace file to a compact CSV file."""
    try:
        # Load the JSON trace file
        with open(trace_file, 'r') as f:
            trace_data = json.load(f)

        # Extract trace events
        events = trace_data.get('traceEvents', [])

        min_timestamp = min(event['ts'] for event in events if ('ts' in event and event.get('ph') == 'B' and event.get('cat') == 'task')) % 1_000_000_000
        print(f"Minimum timestamp (mod 1_000_000_000): {min_timestamp}")
        print(f"first timestamp: {events[0]['ts'] if 'ts' in events[0] else 'N/A'}")
        print(f"second timestamp: {events[1]['ts'] if 'ts' in events[1] else 'N/A'}")
        print(f"reduced first timestamp: {(events[0]['ts'] % 1_000_000_000) - min_timestamp if 'ts' in events[0] else 'N/A'}")
        print(f"reduced second timestamp: {(events[1]['ts'] % 1_000_000_000) - min_timestamp if 'ts' in events[1] else 'N/A'}")
        # Prepare compact CSV output
        with open(output_csv, 'w', newline='') as csvfile:
            fieldnames = ['p_id', 't_id', 'ph', 'ts', 'tuples']  # Shortened field names
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for event in events:
                if event.get('ph') in ['B', 'E'] and event.get('cat') == 'task' and 'args' in event:

                    args = event['args']
                    # Ignore the first 9 digits of the timestamp
                    reduced_ts = (event['ts'] % 1_000_000_000) - min_timestamp
                    writer.writerow({
                        'p_id': args.get('pipeline_id', ''),
                        't_id': args.get('task_id', ''),
                        'ph': event.get('ph', ''),
                        'ts': reduced_ts,
                        'tuples': args.get('tuples', 0) if event['ph'] == 'B' else ''
                    })
        #if os.path.exists(output_csv):
        #    os.remove(trace_file)  # Remove original JSON file if CSV was created successfully
        print(f"Compact CSV file created: {output_csv}")
        return output_csv

    except Exception as e:
        print(f"Error converting JSON to compact CSV: {e}")
        print(f"Failed file: {trace_file.name}")
        return None

def run_benchmark(test_file, output_dir, repeats=2, run_options="all", threads=[4], layouts=None, build_dir="cmake-build-release", project_dir=None):
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

    trace_files_to_process = []


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
                numberOfBuffers = 10000
                if int(buffer_size) >= 20000000: #TODO have variable based on given bufferSize and ops + available memory
                    numberOfBuffers = 5000
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
                    for num_threads in threads:
                        for run in range(1, repeats + 1):
                            run_dir = query_dir
                            if len(layouts) > 1:
                                run_dir = run_dir / f"threads{num_threads}" / f"run{run}_{layout}"
                            else:
                                run_dir = run_dir / f"threads{num_threads}" / f"run{run}"
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
                                    f"--worker.queryEngine.numberOfWorkerThreads={num_threads} "
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
                                    if trace_file.stat().st_size == 0:
                                        print(f"  Trace file is empty for Query {query_id}, Run {run}")
                                        trace_file.unlink()
                                    else:
                                        trace_dest = run_dir / f"threads{num_threads}" / f"GoogleEventTrace_{layout}_threads{num_threads}_buffer{buffer_size}_query{query_id}.json"
                                        shutil.move(trace_file, trace_dest)
                                        trace_files_to_process.append(trace_dest)

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

    #with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    #    executor.map(json_to_csv, trace_files_to_process)


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
    parser.add_argument('--threads', type=parse_int_list, default=[4], help='Number of worker threads to use')
    args = parser.parse_args()

    run_benchmark(
        args.test_file,
        args.output_dir,
        args.repeats,
        args.run_options,
        args.threads,
        build_dir=args.build_dir,
        project_dir=args.project_dir

    )