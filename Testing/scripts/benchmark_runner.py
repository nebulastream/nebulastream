#!/usr/bin/env python3
import argparse
import os
import subprocess
import time
import json
from pathlib import Path
from itertools import product

def run_benchmark(layout, buffer_size, threads, query, output_dir, docker_image="nebulastream/nes-development:local"):
    """Run a single benchmark with the specified parameters"""

    # Create unique identifier for this run
    run_id = f"{int(time.time())}_{layout}_buffer{buffer_size}_threads{threads}_query{query}"
    print(f"Running benchmark: {run_id}")

    # Calculate buffer parameters
    mem_available_bytes = 150000000000  # Could be dynamically calculated
    total_buffer = mem_available_bytes // int(buffer_size)

    # Construct docker command
    docker_opts = "--entrypoint= -v /home/user/.cache/ccache:/home/ubuntu/.ccache/ccache -v /home/user/CLionProjects/nebulastream:/tmp/nebulastream --cap-add SYS_ADMIN --cap-add SYS_PTRACE --rm"

    # Clear previous trace files
    subprocess.run(f"rm -f /home/user/CLionProjects/nebulastream/cmake-build-release/nes-systests/GoogleEventTrace*", shell=True)

    # Run the benchmark
    cmd = f"""docker run {docker_opts} {docker_image} bash -c "
    cd /tmp/nebulastream/cmake-build-release/nes-systests/systest/
    /tmp/nebulastream/cmake-build-release/nes-systests/systest/systest -b \\
      -t /tmp/nebulastream/nes-systests/benchmark/DEBS.test:{query} -- \\
      --worker.queryEngine.numberOfWorkerThreads={threads} \\
      --worker.bufferSizeInBytes={buffer_size} \\
      --worker.numberOfBuffersInGlobalBufferManager={total_buffer} \\
      --worker.defaultQueryExecution.operatorBufferSize={buffer_size} \\
      --worker.defaultQueryExecution.memoryLayout={layout} \\
      --worker.defaultQueryExecution.useSingleMemoryLayout=true \\
      --enableGoogleEventTrace=true
    " """

    subprocess.run(cmd, shell=True)

    # Find and copy the trace file
    result = subprocess.run("find /home/user/CLionProjects/nebulastream/cmake-build-release/nes-systests/ -name \"GoogleEventTrace*\" | sort | tail -n 1",
                            shell=True, capture_output=True, text=True)
    trace_file = result.stdout.strip()

    if trace_file:
        output_file = f"{output_dir}/GoogleEventTrace_{layout}_buffer{buffer_size}_threads{threads}_query{query}.json"
        subprocess.run(f"cp {trace_file} {output_file}", shell=True)
        return output_file
    else:
        print("No GoogleEventTrace file found!")
        return None

def main():
    parser = argparse.ArgumentParser(description='Run NES benchmarks with configurable parameters')
    parser.add_argument('--layouts', nargs='+', default=["COLUMNAR_LAYOUT", "ROW_LAYOUT"],
                        help='Memory layouts to test')
    parser.add_argument('--buffer-sizes', nargs='+', default=["20000000", "400000", "4000"],
                        help='Buffer sizes to test')
    parser.add_argument('--threads', nargs='+', default=["1", "2", "4", "8"],
                        help='Worker thread counts to test')
    parser.add_argument('--queries', nargs='+', default=["01", "02", "03", "04", "05", "06"],
                        help='Queries to run')
    parser.add_argument('--output-dir', default=None,
                        help='Output directory (default: auto-generated benchmark directory)')

    args = parser.parse_args()

    # Create output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        project_dir = "/home/user/CLionProjects/nebulastream"
        # Find the next benchmark ID
        benchmark_dirs = sorted([d for d in os.listdir(f"{project_dir}/Testing/stats")
                                 if d.startswith("benchmark")])
        if benchmark_dirs:
            last_id = int(benchmark_dirs[-1].replace("benchmark", ""))
            benchmark_id = last_id + 1
        else:
            benchmark_id = 1

        output_dir = f"{project_dir}/Testing/stats/benchmark{benchmark_id}"

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Storing benchmark results in {output_dir}")

    # Track completed benchmarks for metadata
    completed = []

    # Run benchmarks
    for layout, buffer_size, threads, query in product(args.layouts, args.buffer_sizes, args.threads, args.queries):
        output_file = run_benchmark(layout, buffer_size, threads, query, output_dir)
        if output_file:
            completed.append({
                'layout': layout,
                'buffer_size': buffer_size,
                'threads': threads,
                'query': query,
                'file': output_file
            })

    # Save metadata about the benchmark run
    with open(f"{output_dir}/benchmark_metadata.json", 'w') as f:
        json.dump({
            'timestamp': int(time.time()),
            'parameters': {
                'layouts': args.layouts,
                'buffer_sizes': args.buffer_sizes,
                'threads': args.threads,
                'queries': args.queries
            },
            'completed': completed
        }, f, indent=2)

    print(f"All benchmarks completed. Results in {output_dir}")
    return output_dir

if __name__ == "__main__":
    main()