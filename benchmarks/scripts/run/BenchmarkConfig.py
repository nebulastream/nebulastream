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
                 num_watermark_gaps_allowed,
                 max_num_sequence_numbers,
                 file_descriptor_buffer_size,
                 min_read_state_size,
                 min_write_state_size,
                 file_operation_time_delta,
                 file_layout,
                 watermark_predictor_type,
                 slice_store_type,
                 query):
        self.num_tuples_to_generate = num_tuples_to_generate
        self.timestamp_increment = timestamp_increment
        self.ingestion_rate = ingestion_rate
        self.no_physical_sources_per_logical_source = no_physical_sources_per_logical_source
        self.number_of_worker_threads = number_of_worker_threads
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.page_size = page_size
        self.num_watermark_gaps_allowed = num_watermark_gaps_allowed
        self.max_num_sequence_numbers = max_num_sequence_numbers
        self.file_descriptor_buffer_size = file_descriptor_buffer_size
        self.min_read_state_size = min_read_state_size
        self.min_write_state_size = min_write_state_size
        self.file_operation_time_delta = file_operation_time_delta
        self.file_layout = file_layout
        self.watermark_predictor_type = watermark_predictor_type
        self.slice_store_type = slice_store_type
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
            "num_watermark_gaps_allowed": self.num_watermark_gaps_allowed,
            "max_num_sequence_numbers": self.max_num_sequence_numbers,
            "file_descriptor_buffer_size": self.file_descriptor_buffer_size,
            "min_read_state_size": self.min_read_state_size,
            "min_write_state_size": self.min_write_state_size,
            "file_operation_time_delta": self.file_operation_time_delta,
            "file_layout": self.file_layout,
            "watermark_predictor_type": self.watermark_predictor_type,
            "slice_store_type": self.slice_store_type,
            "query": self.query,
            "task_queue_size": self.task_queue_size,
            "buffers_in_global_buffer_manager": self.buffers_in_global_buffer_manager,
            "buffers_per_worker": self.buffers_per_worker,
            "buffers_in_source_local_buffer_pool": self.buffers_in_source_local_buffer_pool,
            "throughput_listener_time_interval": self.throughput_listener_time_interval,
            "execution_mode": self.execution_mode
        }


def create_all_benchmark_configs():
    NUM_TUPLES_TO_GENERATE = [0]  # 0 means the source will run indefinitely
    TIMESTAMP_INCREMENT = [1, 10, 100, 1000, 10000]
    INGESTION_RATE = [0, 1000, 10000, 100000, 1000000]  # 0 means the source will ingest tuples as fast as possible

    NO_PHYSICAL_SOURCES = [1]
    NUMBER_OF_WORKER_THREADS = [4]
    BUFFER_SIZES = [4096]
    PAGE_SIZES = [4096]

    NUM_WATERMARK_GAPS_ALLOWED = [10]
    MAX_NUM_SEQUENCE_NUMBERS = [np.iinfo(np.uint64).max]
    FILE_DESCRIPTOR_BUFFER_SIZE = [4096]
    MIN_READ_STATE_SIZE = [0]
    MIN_WRITE_STATE_SIZE = [0]
    FILE_OPERATION_TIME_DELTA = [0]
    FILE_LAYOUT = ["NO_SEPARATION"]
    WATERMARK_PREDICTOR_TYPE = ["KALMAN"]
    SLICE_STORE_TYPE = ["DEFAULT", "FILE_BACKED"]

    # Adding queries with different window sizes.
    WINDOW_SIZE_SLIDE = [
        # Representing a tumbling window of 10s, resulting in 1 concurrent window
        (10 * 1000, 10 * 1000),

        # Representing a sliding window of 10s with slide of 100ms, resulting in 100 concurrent windows
        # (10 * 1000, 100),

        # Representing a sliding window of 1000s with slide of 10s, resulting in 100 concurrent windows
        # (1 * 1000 * 1000, 10 * 1000),

        # Representing a sliding window of 1000s with slide of 100ms, resulting in 10000 concurrent windows
        # (1 * 1000 * 1000, 100)
    ]

    QUERIES = []
    for window_size, slide in WINDOW_SIZE_SLIDE:
        QUERIES.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) INTO csv_sink;"
        )

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
            num_watermark_gaps_allowed,
            max_num_sequence_numbers,
            file_descriptor_buffer_size,
            min_read_state_size,
            min_write_state_size,
            file_operation_time_delta,
            file_layout,
            watermark_predictor_type,
            slice_store_type,
            query
        )
        for num_tuples_to_generate in NUM_TUPLES_TO_GENERATE
        for timestamp_increment in TIMESTAMP_INCREMENT
        for ingestion_rate in INGESTION_RATE
        for no_physical_sources_per_logical_source in NO_PHYSICAL_SOURCES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for buffer_size_in_bytes in BUFFER_SIZES
        for page_size in PAGE_SIZES
        for num_watermark_gaps_allowed in NUM_WATERMARK_GAPS_ALLOWED
        for max_num_sequence_numbers in MAX_NUM_SEQUENCE_NUMBERS
        for file_descriptor_buffer_size in FILE_DESCRIPTOR_BUFFER_SIZE
        for min_read_state_size in MIN_READ_STATE_SIZE
        for min_write_state_size in MIN_WRITE_STATE_SIZE
        for file_operation_time_delta in FILE_OPERATION_TIME_DELTA
        for file_layout in FILE_LAYOUT
        for watermark_predictor_type in WATERMARK_PREDICTOR_TYPE
        for slice_store_type in SLICE_STORE_TYPE
        for query in QUERIES
    ]
