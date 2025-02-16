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
    def __init__(self, number_of_worker_threads, slice_cache_type, slice_store_type, numberOfEntriesSliceCache, lock_slice_cache,
                 timestamp_increment, buffer_size_in_bytes, query, no_physical_sources_per_logical_source):
        self.number_of_worker_threads = number_of_worker_threads
        self.slice_cache_type = slice_cache_type
        self.slice_store_type = slice_store_type
        self.numberOfEntriesSliceCache = numberOfEntriesSliceCache
        self.lock_slice_cache = lock_slice_cache
        self.timestamp_increment = timestamp_increment
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.query = query
        self.no_physical_sources_per_logical_source = no_physical_sources_per_logical_source

        ## Values for configuring the single node worker such that the query showcases the bottleneck
        self.task_queue_size = 100000
        self.buffers_in_global_buffer_manager = 1 * 1000 * 1000
        self.buffers_per_worker = 12800
        self.buffers_in_source_local_buffer_pool = 1000
        self.nautilus_backend = "COMPILER" # "COMPILER" or "INTERPRETER"

    # Returns a dictionary representation of the configuration
    def to_dict(self):
        return {
            "number_of_worker_threads": self.number_of_worker_threads,
            "slice_cache_type": self.slice_cache_type,
            "slice_store_type": self.slice_store_type,
            "numberOfEntriesSliceCache": self.numberOfEntriesSliceCache,
            "lock_slice_cache": self.lock_slice_cache,
            "timestamp_increment": self.timestamp_increment,
            "buffer_size_in_bytes": self.buffer_size_in_bytes,
            "query": self.query,
            "task_queue_size": self.task_queue_size,
            "buffers_in_global_buffer_manager": self.buffers_in_global_buffer_manager,
            "buffers_per_worker": self.buffers_per_worker,
            "buffers_in_source_local_buffer_pool": self.buffers_in_source_local_buffer_pool,
            "nautilus_backend": self.nautilus_backend
        }


def create_all_benchmark_configs():
    NUMBER_OF_WORKER_THREADS = [1, 4, 8]
    CACHE_SIZES = [1, 10, 100]
    CACHE_TYPES = ["LRU", "FIFO", "SECOND_CHANCE"]
    LOCKED_SLICE_CACHE = [False] # True or False
    SLICE_CACHE_TYPE_AND_SIZE = [(type, size, locked_slice_cache) for type in CACHE_TYPES for size in CACHE_SIZES for locked_slice_cache in LOCKED_SLICE_CACHE] + [("NONE", 0, False)]
    SLICE_CACHE_TYPE_AND_SIZE = SLICE_CACHE_TYPE_AND_SIZE[:2]

    SLICE_STORE_TYPE = ["MAP"]  # MAP or LIST
    TIMESTAMP_INCREMENT = [1]
    BUFFER_SIZES = [8196] #[1024, 4096, 8196, 102400]  # 1KB, 4KB, 8KB, 100KB
    QUERIES = []
    NO_PHYSICAL_SOURCES = [1]

    # Adding queries
    WINDOW_SIZE_SLIDE = [(10*1000, 1*1000)]
    for window_size, slide in WINDOW_SIZE_SLIDE:
        QUERIES.append(
            f"SELECT MIN(value) FROM tcp_source WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) INTO csv_sink;"
        )
        # QUERIES.append(
        #     f"SELECT * FROM tcp_source WHERE value > 5000 INTO csv_sink;"
        # )

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
            no_physical_sources_per_logical_source
        )
        for slice_cache_type_size in SLICE_CACHE_TYPE_AND_SIZE
        for slice_store_type in SLICE_STORE_TYPE
        for buffer_size_in_bytes in BUFFER_SIZES
        for timestamp_increment in TIMESTAMP_INCREMENT
        for query in QUERIES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for no_physical_sources_per_logical_source in NO_PHYSICAL_SOURCES
    ]
