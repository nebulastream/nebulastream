#!/usr/bin/env python3
from numpy.lib.format import BUFFER_SIZE


# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This class stores all information needed to run a single benchmarks
class BenchmarkConfig:
    def __init__(self, number_of_worker_threads, slice_cache_type, slice_store_type, slice_cache_size, lock_slice_cache,
                 timestamp_increment, buffer_size_in_bytes, query):
        self.number_of_worker_threads = number_of_worker_threads
        self.slice_cache_type = slice_cache_type
        self.slice_store_type = slice_store_type
        self.slice_cache_size = slice_cache_size
        self.lock_slice_cache = lock_slice_cache
        self.timestamp_increment = timestamp_increment
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.query = query

        ## Values for configuring the single node worker such that the query showcases the bottleneck
        self.task_queue_size = 10000
        self.buffers_in_global_buffer_manager = 102400
        self.buffers_per_worker = 12800
        self.buffers_in_source_local_buffer_pool = 6400
        self.nautilus_backend = "INTERPRETER" # "COMPILER" or "INTERPRETER"


def create_all_benchmark_configs():
    NUMBER_OF_WORKER_THREADS = [4, 1]
    SLICE_CACHE_TYPE_AND_SIZE = [(type, size, locked_slice_cache) for type in ["LRU", "FIFO"] for size in [1, 10, 100] for locked_slice_cache in [True, False]] + [("NO_CACHE", 0, False)]

    SLICE_STORE_TYPE = ["MAP", "LIST"]  # MAP or LIST
    TIMESTAMP_INCREMENT = [1, 10, 1000]
    BUFFER_SIZES = [1024, 4096, 102400]  # 1KB, 4KB, 100KB
    QUERIES = []

    # Adding join queries
    WINDOW_SIZE_SLIDE = [(5, 5), (100, 10)]
    for window_size, slide in WINDOW_SIZE_SLIDE:
        QUERIES.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) INNER JOIN (SELECT * FROM tcp_source2) ON value = value2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) INTO csv_sink;"
        )

    # Returning all possible configurations
    return [
        BenchmarkConfig(
            number_of_worker_threads,
            slice_cache_type_size[0],
            slice_store_type,
            slice_cache_type_size[1],
            slice_cache_type_size[2],
            timestamp_increment,
            buffer_size_in_bytes,
            query,
        )
        for slice_cache_type_size in SLICE_CACHE_TYPE_AND_SIZE
        for slice_store_type in SLICE_STORE_TYPE
        for buffer_size_in_bytes in BUFFER_SIZES
        for timestamp_increment in TIMESTAMP_INCREMENT
        for query in QUERIES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
    ]
