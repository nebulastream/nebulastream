#!/usr/bin/env python3
import os
import subprocess
import argparse
import re
import shutil
from pathlib import Path

def parse_query_params(query_text):
    """Extract parameters from query comment."""
    params = {}

    buffer_match = re.search(r'BufferSize: (\d+)', query_text)
    if buffer_match:
        params['buffer_size'] = buffer_match.group(1)

    op_match = re.search(r'OperatorType: (\w+)', query_text)
    if op_match:
        params['operator_type'] = op_match.group(1)

    # Extract additional parameters based on operator type
    if params.get('operator_type') == 'filter':
        sel_match = re.search(r'Selectivity: (\d+)', query_text)
        if sel_match:
            params['selectivity'] = sel_match.group(1)
    elif params.get('operator_type') == 'map':
        func_match = re.search(r'FunctionType: (\w+)', query_text)
        if func_match:
            params['function_type'] = func_match.group(1)

    # Common parameters
    cols_match = re.search(r'NumColumns: (\d+)', query_text)
    if cols_match:
        params['num_columns'] = cols_match.group(1)

    access_match = re.search(r'AccessedColumns: (\d+)', query_text)
    if access_match:
        params['accessed_columns'] = access_match.group(1)

    return params

def extract_queries(test_file):
    """Extract individual queries from test file."""
    with open(test_file, 'r') as f:
        content = f.read()

    # Split file by queries (sections starting with "# Query")
    query_sections = re.split(r'# Query \d+:', content)

    # First section is the header with source/sink definitions
    header = query_sections[0]

    # Process each query section
    queries = []
    for i, section in enumerate(query_sections[1:], 1):
        query_text = f"# Query {i}:" + section
        params = parse_query_params(query_text)
        params['query_id'] = i

        queries.append({
            'text': query_text,
            'params': params
        })

    return header, queries

def run_benchmark(test_file, output_dir, repeats=2, layouts=None):
    """Run benchmark for all queries with repetitions."""
    if layouts is None:
        layouts = ["ROW_LAYOUT", "COLUMNAR_LAYOUT"]

    # Create benchmark directory structure
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Find next available benchmark number
    existing = [int(d.name.replace("benchmark", "")) for d in output_dir.glob("benchmark*") if d.is_dir()]
    benchmark_num = max(existing) + 1 if existing else 1
    benchmark_dir = output_dir / f"benchmark{benchmark_num}"
    benchmark_dir.mkdir(exist_ok=True)

    # Extract queries from test file
    header, queries = extract_queries(test_file)

    PROJECT_DIR = os.path.abspath(os.getcwd())

    # Process each query
    for query in queries:
        query_id = query['params']['query_id']
        query_dir = benchmark_dir / f"Query{query_id}"
        query_dir.mkdir(exist_ok=True)

        # Save query parameters
        with open(query_dir / "params.txt", 'w') as f:
            for k, v in query['params'].items():
                f.write(f"{k}: {v}\n")

        # Create a temporary test file with just this query
        temp_test_file = query_dir / "query.test"
        with open(temp_test_file, 'w') as f:
            f.write(header)
            f.write(query['text'])

        # Run for each layout
        for layout in layouts:
            for run in range(1, repeats + 1):
                run_dir = query_dir / f"Run{run}_{layout}"
                run_dir.mkdir(exist_ok=True)

                buffer_size = query['params'].get('buffer_size', '4000')

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
                        trace_dest = run_dir / f"GoogleEventTrace_{layout}_buffer{buffer_size}_threads4_query{query_id}.json"
                        shutil.copy(trace_file, trace_dest)
                        print(f"  Saved trace to {trace_dest}")
                    else:
                        print(f"  No trace file found for Query {query_id}, Run {run}")

                except Exception as e:
                    print(f"Error running benchmark: {e}")

    print(f"Benchmark complete. Results in {benchmark_dir}")
    return str(benchmark_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run benchmark tests')
    parser.add_argument('--test-file', required=True, help='Path to test file')
    parser.add_argument('--output-dir', default='benchmark_results', help='Output directory')
    parser.add_argument('--repeats', type=int, default=2, help='Number of repetitions')
    args = parser.parse_args()

    run_benchmark(args.test_file, args.output_dir, args.repeats)