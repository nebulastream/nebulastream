#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import subprocess
import psutil
import time
import matplotlib.pyplot as plt


def monitor_memory_usage(pid, interval=1, timeout=120):
    """Monitor memory usage of a process by its PID."""
    memory_usage = []
    try:
        process = psutil.Process(pid)
        while process.is_running() and timeout > 0:
            mem_info = process.memory_info()
            memory_usage.append(mem_info.rss)  # Resident Set Size
            time.sleep(interval)
            timeout -= 1
    except psutil.NoSuchProcess:
        pass
    return memory_usage


def plot_memory_usage(memory_usage, interval):
    """Plot memory usage over time."""
    plt.plot(memory_usage)
    plt.xlabel('Time (s)')
    plt.ylabel('Memory Usage (bytes)')
    plt.title('Memory Usage Over Time')
    plt.show()


def main():
    # When building with docker toolchain a symlink needs to be created to correctly link testdata (execute in nes root directory): ln -sf $(pwd) /tmp
    command = '$(pwd)/cmake-build-relnologging/nes-systests/systest/systest -t $(pwd)/nes-systests/benchmark/Nexmark.test:05 -b --workingDir=$(pwd)/.cache/benchmarks/working_dir --data $(pwd)/cmake-build-relnologging/nes-systests/testdata --groups large -- --worker.queryOptimizer.sliceStoreType=FILE_BACKED --worker.queryOptimizer.executionMode=COMPILER --worker.queryEngine.numberOfWorkerThreads=4 --worker.numberOfBuffersInGlobalBufferManager=200000'
    process = subprocess.Popen(command, shell=True)

    # Monitor memory usage
    memory_usage = monitor_memory_usage(process.pid, timeout=20)
    print("MemoryUsage: {}", memory_usage)

    # Plot memory usage
    plot_memory_usage(memory_usage, interval=1)


if __name__ == "__main__":
    main()
