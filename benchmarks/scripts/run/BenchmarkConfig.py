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


import re
import numpy as np


## First value of every parameter is the default value
# Source configuration parameters
TIMESTAMP_INCREMENT = [1, 100, 1000, 10000, 100000, 1000000]
INGESTION_RATE = [0, 1000, 10000, 100000, 1000000, 10000000]  # 0 means the source will ingest tuples as fast as possible

# Worker configuration parameters
NUMBER_OF_WORKER_THREADS = [4, 8, 16, 1, 2]
BUFFER_SIZES = [4096, 8192, 32768, 131072, 524288, 1024]
PAGE_SIZES = [4096, 8192, 32768, 131072, 524288, 1024]
WINDOW_SIZE_SLIDE = [
    # Representing a tumbling window of 277.78h, resulting in 1 concurrent window
    (1000 * 1000 * 1000, 1000 * 1000 * 1000),
    # Representing a tumbling window of 2.78h, resulting in 1 concurrent window
    (10 * 1000 * 1000, 10 * 1000 * 1000),
    # Representing a tumbling window of 10s, resulting in 1 concurrent window
    (10 * 1000, 10 * 1000),
    # Representing a sliding window of 277.78h with slide of 16.67min, resulting in 100 concurrent windows
    (1000 * 1000 * 1000, 1000 * 1000),
    # Representing a sliding window of 277.78h with slide of 100s, resulting in 10000 concurrent windows
    (1000 * 1000 * 1000, 100 * 1000),
    # Representing a sliding window of 10s with slide of 100ms, resulting in 100 concurrent windows
    (10 * 1000, 100)
]

SLICE_STORE_TYPE = ["DEFAULT", "FILE_BACKED"]
NUM_WATERMARK_GAPS_ALLOWED = [10, 30, 100, 500, 1000, 1]
MAX_NUM_SEQUENCE_NUMBERS = [np.iinfo(np.uint64).max, 10, 100, 1000]
FILE_DESCRIPTOR_BUFFER_SIZE = [4096, 8192, 32768, 131072, 524288, 1024]
MIN_READ_STATE_SIZE = [0, 64, 128, 512, 1024, 4096, 16384]
MIN_WRITE_STATE_SIZE = [0, 64, 128, 512, 1024, 4096, 16384]
FILE_OPERATION_TIME_DELTA = [0, 1, 10, 100, 1000]
FILE_LAYOUT = ["NO_SEPARATION", "SEPARATE_PAYLOAD", "SEPARATE_KEYS"]
WATERMARK_PREDICTOR_TYPE = ["KALMAN", "RLS", "REGRESSION"]


def get_queries():
    queries = []

    # Simple join with two streams
    for window_size, slide in WINDOW_SIZE_SLIDE:
        queries.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) "
            f"INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )
    # Join with two streams and multiple keys
    for window_size, slide in WINDOW_SIZE_SLIDE:
        queries.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) "
            f"INNER JOIN (SELECT * FROM tcp_source2) ON (id = id2 AND value = value2) WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )

    # Join with two streams and variable sized data as payload
    for window_size, slide in WINDOW_SIZE_SLIDE:
        queries.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) "
            f"INNER JOIN (SELECT * FROM tcp_source4) ON id = id4 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )
    # Join with two streams, multiple keys and variable sized data
    for window_size, slide in WINDOW_SIZE_SLIDE:
        queries.append(
            f"SELECT * FROM (SELECT * FROM tcp_source4) "
            f"INNER JOIN (SELECT * FROM tcp_source5) ON (id4 = id5 AND payload4 = payload5) WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )
    # Join with three streams
    for window_size, slide in WINDOW_SIZE_SLIDE:
        queries.append(
            f"SELECT * FROM (SELECT * FROM tcp_source) "
            f"INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INNER JOIN (SELECT * FROM tcp_source3) ON id = id3 WINDOW SLIDING (timestamp, size {window_size} ms, advance by {slide} ms) "
            f"INTO csv_sink;"
        )

    return queries


# This class stores all information needed to run a single benchmark
class BenchmarkConfig:
    def __init__(self,
                 timestamp_increment,
                 ingestion_rate,
                 number_of_worker_threads,
                 buffer_size_in_bytes,
                 page_size,
                 slice_store_type,
                 num_watermark_gaps_allowed,
                 max_num_sequence_numbers,
                 file_descriptor_buffer_size,
                 min_read_state_size,
                 min_write_state_size,
                 file_operation_time_delta,
                 file_layout,
                 watermark_predictor_type,
                 query):
        self.timestamp_increment = timestamp_increment
        self.ingestion_rate = ingestion_rate
        self.number_of_worker_threads = number_of_worker_threads
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.page_size = page_size
        self.slice_store_type = slice_store_type
        self.num_watermark_gaps_allowed = num_watermark_gaps_allowed
        self.max_num_sequence_numbers = max_num_sequence_numbers
        self.file_descriptor_buffer_size = file_descriptor_buffer_size
        self.min_read_state_size = min_read_state_size
        self.min_write_state_size = min_write_state_size
        self.file_operation_time_delta = file_operation_time_delta
        self.file_layout = file_layout
        self.watermark_predictor_type = watermark_predictor_type
        self.query = query

        ## Values for configuring the single node worker such that the query showcases the bottleneck
        self.task_queue_size = 1000000
        self.buffers_in_global_buffer_manager = 200000
        self.buffers_per_worker = 20000
        self.buffers_in_source_local_buffer_pool = 1000
        self.execution_mode = "COMPILER"  # COMPILER or INTERPRETER

        ## General run configurations
        self.num_tuples_to_generate = 0  # 0 means the source will run indefinitely
        self.no_physical_sources_per_logical_source = 1
        self.throughput_listener_time_interval = 100  # output interval in ms

    # Return a dictionary representation of the configuration
    def to_dict(self):
        return {
            "timestamp_increment": self.timestamp_increment,
            "ingestion_rate": self.ingestion_rate,
            "number_of_worker_threads": self.number_of_worker_threads,
            "buffer_size_in_bytes": self.buffer_size_in_bytes,
            "page_size": self.page_size,
            "slice_store_type": self.slice_store_type,
            "num_watermark_gaps_allowed": self.num_watermark_gaps_allowed,
            "max_num_sequence_numbers": self.max_num_sequence_numbers,
            "file_descriptor_buffer_size": self.file_descriptor_buffer_size,
            "min_read_state_size": self.min_read_state_size,
            "min_write_state_size": self.min_write_state_size,
            "file_operation_time_delta": self.file_operation_time_delta,
            "file_layout": self.file_layout,
            "watermark_predictor_type": self.watermark_predictor_type,
            "query": self.query,
            "task_queue_size": self.task_queue_size,
            "buffers_in_global_buffer_manager": self.buffers_in_global_buffer_manager,
            "buffers_per_worker": self.buffers_per_worker,
            "buffers_in_source_local_buffer_pool": self.buffers_in_source_local_buffer_pool,
            "execution_mode": self.execution_mode
        }


def create_benchmark_configs():
    # Generate configurations where only one parameter varies from the default value
    configs = []
    default_params = {
        "timestamp_increment": TIMESTAMP_INCREMENT[0],
        "ingestion_rate": INGESTION_RATE[0],
        "number_of_worker_threads": NUMBER_OF_WORKER_THREADS[0],
        "buffer_size_in_bytes": BUFFER_SIZES[0],
        "page_size": PAGE_SIZES[0],
        "num_watermark_gaps_allowed": NUM_WATERMARK_GAPS_ALLOWED[0],
        "max_num_sequence_numbers": MAX_NUM_SEQUENCE_NUMBERS[0],
        "file_descriptor_buffer_size": FILE_DESCRIPTOR_BUFFER_SIZE[0],
        "min_read_state_size": MIN_READ_STATE_SIZE[0],
        "min_write_state_size": MIN_WRITE_STATE_SIZE[0],
        "file_operation_time_delta": FILE_OPERATION_TIME_DELTA[0],
        "file_layout": FILE_LAYOUT[0],
        "watermark_predictor_type": WATERMARK_PREDICTOR_TYPE[0],
        "query": get_queries()[0]
    }
    shared_params = {
        "timestamp_increment": TIMESTAMP_INCREMENT,
        "ingestion_rate": INGESTION_RATE,
        "number_of_worker_threads": NUMBER_OF_WORKER_THREADS,
        "buffer_size_in_bytes": BUFFER_SIZES,
        "page_size": PAGE_SIZES,
        "query": get_queries()
    }
    file_backed_params = {
        "num_watermark_gaps_allowed": NUM_WATERMARK_GAPS_ALLOWED,
        "max_num_sequence_numbers": MAX_NUM_SEQUENCE_NUMBERS,
        "file_descriptor_buffer_size": FILE_DESCRIPTOR_BUFFER_SIZE,
        "min_read_state_size": MIN_READ_STATE_SIZE,
        "min_write_state_size": MIN_WRITE_STATE_SIZE,
        "file_operation_time_delta": FILE_OPERATION_TIME_DELTA,
        "file_layout": FILE_LAYOUT,
        "watermark_predictor_type": WATERMARK_PREDICTOR_TYPE
    }

    # Choose certain timestamp_increment values as default
    default_timestamp_increments = [1, 1000]

    # Choose certain combinations of window size and slide from the first query as default
    default_queries = []
    window_pattern = r"WINDOW SLIDING \(timestamp, size (\d+) ms, advance by (\d+) ms\)"
    for query in get_queries()[:len(WINDOW_SIZE_SLIDE)]:
        size_and_slide = re.search(window_pattern, query)
        size = int(size_and_slide.group(1))
        # slide = int(size_and_slide.group(2))

        if size != 10000:
            default_queries.append(query)

    print(f"number of default configs: {len(default_queries) * len(default_timestamp_increments)}")

    # Generate configurations for each shared parameter
    for param, values in shared_params.items():
        for value in values:
            for slice_store_type in SLICE_STORE_TYPE:
                config_params = default_params.copy()
                config_params[param] = value
                config_params["slice_store_type"] = slice_store_type
                configs.append(BenchmarkConfig(**config_params))
    # Generate configurations for each file backed parameter
    for param, values in file_backed_params.items():
        for value in values:
            config_params = default_params.copy()
            config_params[param] = value
            config_params["slice_store_type"] = "FILE_BACKED"
            configs.append(BenchmarkConfig(**config_params))

    # Generate configurations for each default combination of timestamp_increment and query, excluding default_params
    for timestamp_increment in default_timestamp_increments:
        if timestamp_increment == default_params['timestamp_increment']:
            continue
        for query in default_queries:
            if query == default_params['query']:
                continue
            for param, values in shared_params.items():
                for value in values:
                    for slice_store_type in SLICE_STORE_TYPE:
                        config_params = default_params.copy()
                        config_params["timestamp_increment"] = timestamp_increment
                        config_params["query"] = query
                        config_params[param] = value
                        config_params["slice_store_type"] = slice_store_type
                        configs.append(BenchmarkConfig(**config_params))
            for param, values in file_backed_params.items():
                for value in values:
                    config_params = default_params.copy()
                    config_params["timestamp_increment"] = timestamp_increment
                    config_params["query"] = query
                    config_params[param] = value
                    config_params["slice_store_type"] = "FILE_BACKED"
                    configs.append(BenchmarkConfig(**config_params))

    return configs


def create_all_benchmark_configs():
    # Generate all possible configurations
    return [
        BenchmarkConfig(
            timestamp_increment,
            ingestion_rate,
            number_of_worker_threads,
            buffer_size_in_bytes,
            page_size,
            slice_store_type,
            num_watermark_gaps_allowed,
            max_num_sequence_numbers,
            file_descriptor_buffer_size,
            min_read_state_size,
            min_write_state_size,
            file_operation_time_delta,
            file_layout,
            watermark_predictor_type,
            query
        )
        for timestamp_increment in TIMESTAMP_INCREMENT
        for ingestion_rate in INGESTION_RATE
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for buffer_size_in_bytes in BUFFER_SIZES
        for page_size in PAGE_SIZES
        for slice_store_type in SLICE_STORE_TYPE
        for num_watermark_gaps_allowed in (NUM_WATERMARK_GAPS_ALLOWED if slice_store_type != "DEFAULT" else [0])
        for max_num_sequence_numbers in (MAX_NUM_SEQUENCE_NUMBERS if slice_store_type != "DEFAULT" else [0])
        for file_descriptor_buffer_size in (FILE_DESCRIPTOR_BUFFER_SIZE if slice_store_type != "DEFAULT" else [0])
        for min_read_state_size in (MIN_READ_STATE_SIZE if slice_store_type != "DEFAULT" else [0])
        for min_write_state_size in (MIN_WRITE_STATE_SIZE if slice_store_type != "DEFAULT" else [0])
        for file_operation_time_delta in (FILE_OPERATION_TIME_DELTA if slice_store_type != "DEFAULT" else [0])
        for file_layout in (FILE_LAYOUT if slice_store_type != "DEFAULT" else ["NO_SEPARATION"])
        for watermark_predictor_type in (WATERMARK_PREDICTOR_TYPE if slice_store_type != "DEFAULT" else ["KALMAN"])
        for query in get_queries()
    ]
