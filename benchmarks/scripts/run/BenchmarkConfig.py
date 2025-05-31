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


import numpy as np


# This class stores all information needed to run a single benchmark
class BenchmarkConfig:
    def __init__(self,
                 num_tuples_to_generate,
                 timestamp_increment,
                 ingestion_rate,
                 no_physical_sources_per_logical_source,
                 number_of_worker_threads,
                 buffer_size_in_bytes,
                 page_size,
                 query):
        self.num_tuples_to_generate = num_tuples_to_generate
        self.timestamp_increment = timestamp_increment
        self.ingestion_rate = ingestion_rate
        self.no_physical_sources_per_logical_source = no_physical_sources_per_logical_source
        self.number_of_worker_threads = number_of_worker_threads
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.page_size = page_size
        self.query = query

        ## Values for configuring the single node worker such that the query showcases the bottleneck
        self.task_queue_size = 100000
        self.buffers_in_global_buffer_manager = 200000
        self.buffers_per_worker = 20000
        self.buffers_in_source_local_buffer_pool = 1000
        self.throughput_listener_time_interval = 500
        self.execution_mode = "COMPILER"

    # Return a dictionary representation of the configuration
    def to_dict(self):
        return {
            "timestamp_increment": self.timestamp_increment,
            "ingestion_rate": self.ingestion_rate,
            "number_of_worker_threads": self.number_of_worker_threads,
            "buffer_size_in_bytes": self.buffer_size_in_bytes,
            "page_size": self.page_size,
            "query": self.query,
            "task_queue_size": self.task_queue_size,
            "buffers_in_global_buffer_manager": self.buffers_in_global_buffer_manager,
            "buffers_per_worker": self.buffers_per_worker,
            "buffers_in_source_local_buffer_pool": self.buffers_in_source_local_buffer_pool,
            "throughput_listener_time_interval": self.throughput_listener_time_interval,
            "execution_mode": self.execution_mode
        }


def create_all_benchmark_configs():
    NUM_TUPLES_TO_GENERATE = [200000]  # 0 means the source will run indefinitely
    TIMESTAMP_INCREMENT = [1]
    INGESTION_RATE = [50000]  # 0 means the source will ingest tuples as fast as possible

    NO_PHYSICAL_SOURCES = [1]
    NUMBER_OF_WORKER_THREADS = [4]
    BUFFER_SIZES = [4096]
    PAGE_SIZES = [4096]

    # Adding queries with different window sizes.
    WINDOW_SIZE_SLIDE = [
        # Representing a tumbling window of 10s, resulting in 1 concurrent window
        # (10 * 1000, 10 * 1000),

        # Representing a tumbling window of 277.78h, resulting in 1 concurrent window
        (1000 * 1000 * 1000, 1000 * 1000 * 1000),

        # Representing a sliding window of 10s with slide of 100ms, resulting in 100 concurrent windows
        # (10 * 1000, 100),

        # Representing a sliding window of 1000s with slide of 10s, resulting in 100 concurrent windows
        # (1 * 1000 * 1000, 10 * 1000),

        # Representing a sliding window of 1000s with slide of 100ms, resulting in 10000 concurrent windows
        # (1 * 1000 * 1000, 100)
    ]

    QUERIES = []

    # Simple join with two streams
    # for window_size, slide in WINDOW_SIZE_SLIDE:
    #     QUERIES.append(
    #         f"SELECT * FROM (SELECT * FROM tcp_source) "
    #         f"INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
    #         f"INTO csv_sink;"
    #     )

    # Join with two streams and multiple keys
    # for window_size, slide in WINDOW_SIZE_SLIDE:
    #     QUERIES.append(
    #         f"SELECT * FROM (SELECT * FROM tcp_source) "
    #         f"INNER JOIN (SELECT * FROM tcp_source2) ON (id = id2 AND value = value2) WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
    #         f"INTO csv_sink;"
    #     )

    # Join with two streams and variable sized data
    # for window_size, slide in WINDOW_SIZE_SLIDE:
    #     QUERIES.append(
    #         f"SELECT * FROM (SELECT * FROM tcp_source) "
    #         f"INNER JOIN (SELECT * FROM tcp_source4) ON id = id4 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
    #         f"INTO csv_sink;"
    #     )

    # Join with two streams, multiple keys and variable sized data
    for window_size, slide in WINDOW_SIZE_SLIDE:
        QUERIES.append(
            f"SELECT * FROM (SELECT * FROM tcp_source4) "
            f"INNER JOIN (SELECT * FROM tcp_source5) ON (id4 = id5 AND payload4 = payload5) WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )

    # Join with three streams
    # for window_size, slide in WINDOW_SIZE_SLIDE:
    #     QUERIES.append(
    #         f"SELECT * FROM (SELECT * FROM tcp_source) "
    #         f"INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
    #         f"INNER JOIN (SELECT * FROM tcp_source3) ON id = id3 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
    #         f"INTO csv_sink;"
    #     )

    # Return all possible configurations
    return [
        BenchmarkConfig(
            num_tuples_to_generate,
            timestamp_increment,
            ingestion_rate,
            no_physical_sources_per_logical_source,
            number_of_worker_threads,
            buffer_size_in_bytes,
            page_size,
            query
        )
        for num_tuples_to_generate in NUM_TUPLES_TO_GENERATE
        for timestamp_increment in TIMESTAMP_INCREMENT
        for ingestion_rate in INGESTION_RATE
        for no_physical_sources_per_logical_source in NO_PHYSICAL_SOURCES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for buffer_size_in_bytes in BUFFER_SIZES
        for page_size in PAGE_SIZES
        for query in QUERIES
    ]
