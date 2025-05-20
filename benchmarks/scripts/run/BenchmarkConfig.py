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

#from numpy.lib.format import BUFFER_SIZE


# This class stores all information needed to run a single benchmark
class BenchmarkConfig:
    def __init__(self, number_of_worker_threads, timestamp_increment, ingestion_rate, buffer_size_in_bytes, query,
                 no_physical_sources_per_logical_source):
        self.number_of_worker_threads = number_of_worker_threads
        self.timestamp_increment = timestamp_increment
        self.ingestion_rate = ingestion_rate
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.query = query
        self.no_physical_sources_per_logical_source = no_physical_sources_per_logical_source

        ## Values for configuring the single node worker such that the query showcases the bottleneck
        self.task_queue_size = 100000
        self.buffers_in_global_buffer_manager = 1 * 1000 * 1000
        self.buffers_per_worker = 12800
        self.buffers_in_source_local_buffer_pool = 1000
        self.nautilus_backend = "COMPILER"  # "COMPILER" or "INTERPRETER"

    # Return a dictionary representation of the configuration
    def to_dict(self):
        return {
            "number_of_worker_threads": self.number_of_worker_threads,
            "timestamp_increment": self.timestamp_increment,
            "ingestion_rate": self.ingestion_rate,
            "buffer_size_in_bytes": self.buffer_size_in_bytes,
            "query": self.query,
            "task_queue_size": self.task_queue_size,
            "buffers_in_global_buffer_manager": self.buffers_in_global_buffer_manager,
            "buffers_per_worker": self.buffers_per_worker,
            "buffers_in_source_local_buffer_pool": self.buffers_in_source_local_buffer_pool,
            "nautilus_backend": self.nautilus_backend
        }


def create_all_benchmark_configs():
    NUMBER_OF_WORKER_THREADS = [4]
    TIMESTAMP_INCREMENT = [1]
    INGESTION_RATE = [0]
    BUFFER_SIZES = [4096]  # [1024, 4096, 8196, 102400]  # 1KB, 4KB, 8KB, 100KB
    QUERIES = []
    NO_PHYSICAL_SOURCES = [1]

    # Adding queries with different window sizes.
    WINDOW_SIZE_SLIDE = [
        # Representing a tumbling window of 10s, resulting in 1 concurrent window
        (10 * 1000, 10 * 1000),

        # Representing a sliding window of 10s with slide of 100ms, resulting in 100 concurrent windows
        (10 * 1000, 100),

        # Representing a sliding window of 1000s with slide of 10s, resulting in 100 concurrent windows
        (1 * 1000 * 1000, 10 * 1000),

        # Representing a sliding window of 1000s with slide of 100ms, resulting in 10000 concurrent windows
        (1 * 1000 * 1000, 100)
    ]

    for window_size, slide in WINDOW_SIZE_SLIDE:
        QUERIES.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) INTO csv_sink;"
        )

    # Return all possible configurations
    return [
        BenchmarkConfig(
            number_of_worker_threads,
            timestamp_increment,
            ingestion_rate,
            buffer_size_in_bytes,
            query,
            no_physical_sources_per_logical_source
        )
        for buffer_size_in_bytes in BUFFER_SIZES
        for timestamp_increment in TIMESTAMP_INCREMENT
        for ingestion_rate in INGESTION_RATE
        for query in QUERIES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for no_physical_sources_per_logical_source in NO_PHYSICAL_SOURCES
    ]
