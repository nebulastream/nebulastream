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


## First value of every parameter is the default value
# Source configuration parameters
BATCH_SIZES = [1]
TIMESTAMP_INCREMENTS = [1]
INGESTION_RATES = [0]  # 0 means the source will ingest tuples as fast as possible
MATCH_RATES = [70]  # match rate in percent, values > 100 simply use a counter for every server

# Shared worker configuration parameters
NUMBER_OF_WORKER_THREADS = [4]
BUFFER_SIZES = [4096]
PAGE_SIZES = [4096]
WINDOW_SIZE_SLIDE = [
    # Representing a tumbling window of 10s, resulting in 1 concurrent window
    (10 * 1000, 10 * 1000)
]


def get_queries():
    queries = []

    # Simple join with two streams
    for window_size, slide in WINDOW_SIZE_SLIDE:
        queries.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) "
            f"INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )

    return queries


# This class stores all information needed to run a single benchmark
class BenchmarkConfig:
    def __init__(self,
                 batch_size,
                 timestamp_increment,
                 ingestion_rate,
                 match_rate,
                 number_of_worker_threads,
                 buffer_size_in_bytes,
                 page_size,
                 query,
                 task_queue_size=100000,
                 buffers_in_global_buffer_manager=200000,
                 buffers_per_worker=20000,
                 buffers_in_source_local_buffer_pool=1000,
                 execution_mode="COMPILER"):
        self.batch_size = batch_size
        self.timestamp_increment = timestamp_increment
        self.ingestion_rate = ingestion_rate
        self.match_rate = match_rate
        self.number_of_worker_threads = number_of_worker_threads
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.page_size = page_size
        self.query = query

        ## Values for configuring the single node worker such that the query showcases the bottleneck
        self.task_queue_size = task_queue_size
        self.buffers_in_global_buffer_manager = buffers_in_global_buffer_manager
        self.buffers_per_worker = buffers_per_worker
        self.buffers_in_source_local_buffer_pool = buffers_in_source_local_buffer_pool
        self.execution_mode = execution_mode  # COMPILER or INTERPRETER

        ## General run configurations
        self.num_tuples_to_generate = 0  # 0 means the source will run indefinitely
        self.no_physical_sources_per_logical_source = 1
        self.throughput_listener_time_interval = 1000  # output interval in ms

    # Return a dictionary representation of the configuration
    def to_dict(self):
        return {
            "batch_size": self.batch_size,
            "timestamp_increment": self.timestamp_increment,
            "ingestion_rate": self.ingestion_rate,
            "match_rate": self.match_rate,
            "number_of_worker_threads": self.number_of_worker_threads,
            "buffer_size_in_bytes": self.buffer_size_in_bytes,
            "page_size": self.page_size,
            "query": self.query,
            "task_queue_size": self.task_queue_size,
            "buffers_in_global_buffer_manager": self.buffers_in_global_buffer_manager,
            "buffers_per_worker": self.buffers_per_worker,
            "buffers_in_source_local_buffer_pool": self.buffers_in_source_local_buffer_pool,
            "execution_mode": self.execution_mode
        }


def create_configs():
    # Generate all possible configurations
    return [
        BenchmarkConfig(
            batch_size,
            timestamp_increment,
            ingestion_rate,
            match_rate,
            number_of_worker_threads,
            buffer_size_in_bytes,
            page_size,
            query
        )
        for batch_size in BATCH_SIZES
        for timestamp_increment in TIMESTAMP_INCREMENTS
        for ingestion_rate in INGESTION_RATES
        for match_rate in MATCH_RATES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for buffer_size_in_bytes in BUFFER_SIZES
        for page_size in PAGE_SIZES
        for query in get_queries()
    ]
