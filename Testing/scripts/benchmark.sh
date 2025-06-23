#!/bin/bash
#run from nebulastream directory

# Define parameter arrays
memory_layouts=("COLUMNAR_LAYOUT" "ROW_LAYOUT")
buffer_sizes=("40000000" "400000" "4000")
worker_threads=("1" "2" "4" "8")
debs_queries=("01" "02" "03" "04" "05" "06")

PROJECT_DIR="/home/user/CLionProjects/nebulastream"

# Create stats directory if it doesn't exist
mkdir -p "$PROJECT_DIR/Testing/stats"

# Docker container configuration
DOCKER_IMAGE="nebulastream/nes-development:local"
DOCKER_OPTS="--entrypoint= -v /home/user/.cache/ccache:/home/ubuntu/.ccache/ccache -v /home/user/CLionProjects/nebulastream:/tmp/nebulastream --cap-add SYS_ADMIN --cap-add SYS_PTRACE --rm"
#--cpuset-cpus 0-7
#incrementing benchmark id for each run
LAST_BENCHMARK=$(find "$PROJECT_DIR/Testing/stats" -maxdepth 1 -type d -name "benchmark*" | sort -V | tail -n 1)
if [ -z "$LAST_BENCHMARK" ]; then
    BENCHMARK_ID=1
else
    BENCHMARK_ID=$(($(echo "$LAST_BENCHMARK" | grep -o '[0-9]*$')+1))
fi
BENCHMARK_DIR="$PROJECT_DIR/Testing/stats/benchmark$BENCHMARK_ID"
mkdir -p "$BENCHMARK_DIR"
echo "Storing benchmark results in $BENCHMARK_DIR"

# Run tests with different parameter combinations
for layout in "${memory_layouts[@]}"; do
  for buffer_size in "${buffer_sizes[@]}"; do
    for threads in "${worker_threads[@]}"; do
      for query in "${debs_queries[@]}"; do
        echo "====================================================================="
        echo "Running with layout=$layout, buffer_size=$buffer_size, threads=$threads, query=$query"
        echo "====================================================================="


        # Create a unique identifier for this run
        RUN_ID="$(date +%s)_${layout}_buffer${buffer_size}_threads${threads}_query${query}"

        # clear enginestats files before running the test
        rm -f $PROJECT_DIR/cmake-build-release/nes-systests/enginestats*

        memAvailableInBytes=150000000000 #$(grep MemAvailable /proc/meminfo | awk '{print $2 * 1024}') # Convert kB to bytes
        totalBuffer=$((memAvailableInBytes / buffer_size))
        echo "Total buffers available: $totalBuffer"

        # Run the Docker container with the specified parameters
        docker run $DOCKER_OPTS $DOCKER_IMAGE bash -c "
        # Print current working directory for debugging
        echo 'Current working directory: '\$(pwd)

        # Important: Change to the same working directory that CLion uses
        cd /tmp/nebulastream/cmake-build-release/nes-systests/systest/
        echo 'Changed to working directory: '\$(pwd)
        #Run the system test
        /tmp/nebulastream/cmake-build-release/nes-systests/systest/systest -b \
          -t /tmp/nebulastream/nes-systests/benchmark/DEBS.test:$query -- \
          --worker.queryEngine.numberOfWorkerThreads=$threads \
          --worker.bufferSizeInBytes=$buffer_size \
          --worker.numberOfBuffersPerWorker=256 \
          --worker.numberOfBuffersInGlobalBufferManager=$totalBuffer \
          --worker.queryOptimizer.operatorBufferSize=$buffer_size \
          --worker.queryOptimizer.memoryLayout=$layout \
          --worker.queryOptimizer.useSingleMemoryLayout=true
        "

        # get last (only) enginestat file
        enginestat=$(find "$PROJECT_DIR/cmake-build-release/nes-systests/" -name "enginestats*" -o -name "EngineStats*" -type f | sort | tail -n 1)
        echo "Found enginestats file: $enginestat"
        if [ -n "$enginestat" ]; then
          # Create a new filename with parameters
          new_filename="${BENCHMARK_DIR}/enginestats_${layout}_buffer${buffer_size}_threads${threads}_query${query}.stats"

          # Copy the file with the new name
          cp "$enginestat" "$new_filename"
          echo "Copied stats from $enginestat to $new_filename"
        else
          echo "No new enginestats file found!"
        fi

        # Add a small delay to ensure files are properly written
        sleep 2
      done
    done
  done
done

echo "All tests completed. Results in $(BENCHMARK_DIR) directory."

echo "reading enginestats files"

python3 "$PROJECT_DIR/Testing/scripts/enginestatsread.py" "$BENCHMARK_DIR"

echo "parsing results to csv"

python3 "$PROJECT_DIR/Testing/scripts/parse_results.py" "$BENCHMARK_DIR"

echo "results saved in $BENCHMARK_DIR/benchmark${BENCHMARK_ID}.csv"

echo "plotting results"

python3 "$PROJECT_DIR/Testing/scripts/plots.py" "$BENCHMARK_DIR/benchmark${BENCHMARK_ID}.csv"