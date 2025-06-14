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
TIMESTAMP_INCREMENTS = [1, 100, 1000, 10000, 100000, 1000000]
INGESTION_RATES = [0, 1000, 10000, 100000, 1000000, 10000000]  # 0 means the source will ingest tuples as fast as possible

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

SLICE_STORE_TYPES = ["DEFAULT", "FILE_BACKED"]
FILE_DESCRIPTOR_BUFFER_SIZES = [4096, 8192, 32768, 131072, 524288, 1024]
NUM_WATERMARK_GAPS_ALLOWED = [10, 30, 100, 500, 1000, 1]
MAX_NUM_SEQUENCE_NUMBERS = [np.iinfo(np.uint64).max, 10, 100, 1000]
MIN_READ_STATE_SIZES = [0, 64, 128, 512, 1024, 4096, 16384]
MIN_WRITE_STATE_SIZES = [0, 64, 128, 512, 1024, 4096, 16384]
FILE_OPERATION_TIME_DELTAS = [0, 1, 10, 100, 1000]
MAX_MEMORY_CONSUMPTION = [np.iinfo(np.uint64).max, 128 * 1024 * 1024, 4 * 1024 * 1024, 1024 * 1024, 512 * 1024, 64 * 1024]
MEMORY_MODELS = ["PREDICT_WATERMARKS", "WITHIN_BUDGET", "ADAPTIVE", "DEFAULT"]
FILE_LAYOUTS = ["NO_SEPARATION", "SEPARATE_PAYLOAD", "SEPARATE_KEYS"]
WATERMARK_PREDICTOR_TYPES = ["KALMAN", "RLS", "REGRESSION"]


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


def get_default_params():
    return {
        "timestamp_increment": TIMESTAMP_INCREMENTS[0],
        "ingestion_rate": INGESTION_RATES[0],
        "number_of_worker_threads": NUMBER_OF_WORKER_THREADS[0],
        "buffer_size_in_bytes": BUFFER_SIZES[0],
        "page_size": PAGE_SIZES[0],
        "file_descriptor_buffer_size": FILE_DESCRIPTOR_BUFFER_SIZES[0],
        "num_watermark_gaps_allowed": NUM_WATERMARK_GAPS_ALLOWED[0],
        "max_num_sequence_numbers": MAX_NUM_SEQUENCE_NUMBERS[0],
        "min_read_state_size": MIN_READ_STATE_SIZES[0],
        "min_write_state_size": MIN_WRITE_STATE_SIZES[0],
        "file_operation_time_delta": FILE_OPERATION_TIME_DELTAS[0],
        "max_memory_consumption": MAX_MEMORY_CONSUMPTION[0],
        "memory_model": MEMORY_MODELS[0],
        "file_layout": FILE_LAYOUTS[0],
        "watermark_predictor_type": WATERMARK_PREDICTOR_TYPES[0],
        "query": get_queries()[0]
    }


# This class stores all information needed to run a single benchmark
class BenchmarkConfig:
    def __init__(self,
                 timestamp_increment,
                 ingestion_rate,
                 number_of_worker_threads,
                 buffer_size_in_bytes,
                 page_size,
                 slice_store_type,
                 file_descriptor_buffer_size,
                 num_watermark_gaps_allowed,
                 max_num_sequence_numbers,
                 min_read_state_size,
                 min_write_state_size,
                 file_operation_time_delta,
                 max_memory_consumption,
                 memory_model,
                 file_layout,
                 watermark_predictor_type,
                 query):
        self.timestamp_increment = timestamp_increment
        self.ingestion_rate = ingestion_rate
        self.number_of_worker_threads = number_of_worker_threads
        self.buffer_size_in_bytes = buffer_size_in_bytes
        self.page_size = page_size
        self.slice_store_type = slice_store_type
        self.file_descriptor_buffer_size = file_descriptor_buffer_size
        self.num_watermark_gaps_allowed = num_watermark_gaps_allowed
        self.max_num_sequence_numbers = max_num_sequence_numbers
        self.min_read_state_size = min_read_state_size
        self.min_write_state_size = min_write_state_size
        self.file_operation_time_delta = file_operation_time_delta
        self.max_memory_consumption = max_memory_consumption
        self.memory_model = memory_model
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
            "file_descriptor_buffer_size": self.file_descriptor_buffer_size,
            "num_watermark_gaps_allowed": self.num_watermark_gaps_allowed,
            "max_num_sequence_numbers": self.max_num_sequence_numbers,
            "min_read_state_size": self.min_read_state_size,
            "min_write_state_size": self.min_write_state_size,
            "file_operation_time_delta": self.file_operation_time_delta,
            "max_memory_consumption": self.max_memory_consumption,
            "memory_model": self.memory_model,
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
    default_params = get_default_params()
    shared_params = {
        "timestamp_increment": TIMESTAMP_INCREMENTS,
        "ingestion_rate": INGESTION_RATES,
        "number_of_worker_threads": NUMBER_OF_WORKER_THREADS,
        "buffer_size_in_bytes": BUFFER_SIZES,
        "page_size": PAGE_SIZES,
        "query": get_queries()
    }
    file_backed_params = {
        "file_descriptor_buffer_size": FILE_DESCRIPTOR_BUFFER_SIZES,
        "num_watermark_gaps_allowed": NUM_WATERMARK_GAPS_ALLOWED,
        "max_num_sequence_numbers": MAX_NUM_SEQUENCE_NUMBERS,
        "min_read_state_size": MIN_READ_STATE_SIZES,
        "min_write_state_size": MIN_WRITE_STATE_SIZES,
        "file_operation_time_delta": FILE_OPERATION_TIME_DELTAS,
        "max_memory_consumption": MAX_MEMORY_CONSUMPTION,
        "memory_model": MEMORY_MODELS,
        "file_layout": FILE_LAYOUTS,
        "watermark_predictor_type": WATERMARK_PREDICTOR_TYPES
    }

    # Set some timestamp_increment values as default
    default_timestamp_increments = [1, 1000]

    # Set some combinations of window size and slide from the first query as default
    default_queries = []
    window_pattern = r"WINDOW SLIDING \(timestamp, size (\d+) ms, advance by (\d+) ms\)"
    for query in get_queries()[:len(WINDOW_SIZE_SLIDE)]:
        size_and_slide = re.search(window_pattern, query)
        size = int(size_and_slide.group(1))
        # slide = int(size_and_slide.group(2))
        if size != 10000:
            default_queries.append(query)

    # Generate configurations for each shared parameter (one per value)
    for param, values in shared_params.items():
        for value in values:
            for slice_store_type in SLICE_STORE_TYPES:
                config_params = default_params.copy()
                config_params[param] = value
                config_params["slice_store_type"] = slice_store_type
                configs.append(BenchmarkConfig(**config_params))
    # Generate configurations for each file backed parameter (one per value)
    for param, values in file_backed_params.items():
        for value in values:
            config_params = default_params.copy()
            config_params[param] = value
            config_params["slice_store_type"] = "FILE_BACKED"
            configs.append(BenchmarkConfig(**config_params))

    # Generate configurations for each default combination of timestamp_increment and query, excluding default_params
    for timestamp_increment in default_timestamp_increments:
        for query in default_queries:
            if timestamp_increment == default_params['timestamp_increment'] and query == default_params['query']:
                continue
            for param, values in shared_params.items():
                for value in values:
                    for slice_store_type in SLICE_STORE_TYPES:
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


def create_watermark_predictor_benchmark_configs():
    # Generate all possible configurations for watermark predictor parameters
    default_params = get_default_params()
    del default_params["watermark_predictor_type"]
    del default_params["num_watermark_gaps_allowed"]
    del default_params["max_num_sequence_numbers"]
    del default_params["min_read_state_size"]
    del default_params["min_write_state_size"]
    del default_params["file_operation_time_delta"]

    return [
        BenchmarkConfig(**default_params,
                        slice_store_type=slice_store_type,
                        watermark_predictor_type=watermark_predictor_type,
                        num_watermark_gaps_allowed=num_watermark_gaps_allowed,
                        max_num_sequence_numbers=max_num_sequence_numbers,
                        min_read_state_size=min_read_state_size,
                        min_write_state_size=min_write_state_size,
                        file_operation_time_delta=file_operation_time_delta)
        for slice_store_type in ["FILE_BACKED"]
        for watermark_predictor_type in WATERMARK_PREDICTOR_TYPES
        for num_watermark_gaps_allowed in NUM_WATERMARK_GAPS_ALLOWED
        for max_num_sequence_numbers in MAX_NUM_SEQUENCE_NUMBERS
        for min_read_state_size in MIN_READ_STATE_SIZES
        for min_write_state_size in MIN_WRITE_STATE_SIZES
        for file_operation_time_delta in FILE_OPERATION_TIME_DELTAS
    ]


def create_memory_model_benchmark_configs():
    # Generate all possible configurations for memory model parameters
    default_params = get_default_params()
    del default_params["memory_model"]
    del default_params["max_memory_consumption"]

    return [
        BenchmarkConfig(**default_params, slice_store_type=slice_store_type, memory_model=memory_model,
                        max_memory_consumption=max_memory_consumption)
        for slice_store_type in ["FILE_BACKED"]
        for memory_model in MEMORY_MODELS
        for max_memory_consumption in (MAX_MEMORY_CONSUMPTION if memory_model == "WITHIN_BUDGET" or memory_model == "ADAPTIVE" else [MAX_MEMORY_CONSUMPTION[0]])
    ]


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
            file_descriptor_buffer_size,
            num_watermark_gaps_allowed,
            max_num_sequence_numbers,
            min_read_state_size,
            min_write_state_size,
            file_operation_time_delta,
            max_memory_consumption,
            memory_model,
            file_layout,
            watermark_predictor_type,
            query
        )
        for timestamp_increment in TIMESTAMP_INCREMENTS
        for ingestion_rate in INGESTION_RATES
        for number_of_worker_threads in NUMBER_OF_WORKER_THREADS
        for buffer_size_in_bytes in BUFFER_SIZES
        for page_size in PAGE_SIZES
        for slice_store_type in SLICE_STORE_TYPES
        for file_descriptor_buffer_size in (FILE_DESCRIPTOR_BUFFER_SIZES if slice_store_type == "FILE_BACKED" else [FILE_DESCRIPTOR_BUFFER_SIZES[0]])
        for num_watermark_gaps_allowed in (NUM_WATERMARK_GAPS_ALLOWED if slice_store_type == "FILE_BACKED" else [NUM_WATERMARK_GAPS_ALLOWED[0]])
        for max_num_sequence_numbers in (MAX_NUM_SEQUENCE_NUMBERS if slice_store_type == "FILE_BACKED" else [MAX_NUM_SEQUENCE_NUMBERS[0]])
        for min_read_state_size in (MIN_READ_STATE_SIZES if slice_store_type == "FILE_BACKED" else [MIN_READ_STATE_SIZES[0]])
        for min_write_state_size in (MIN_WRITE_STATE_SIZES if slice_store_type == "FILE_BACKED" else [MIN_WRITE_STATE_SIZES[0]])
        for file_operation_time_delta in (FILE_OPERATION_TIME_DELTAS if slice_store_type == "FILE_BACKED" else [FILE_OPERATION_TIME_DELTAS[0]])
        for max_memory_consumption in (MAX_MEMORY_CONSUMPTION if slice_store_type == "FILE_BACKED" else [MAX_MEMORY_CONSUMPTION[0]])
        for memory_model in (MEMORY_MODELS if slice_store_type == "FILE_BACKED" else [MEMORY_MODELS[0]])
        for file_layout in (FILE_LAYOUTS if slice_store_type == "FILE_BACKED" else [FILE_LAYOUTS[0]])
        for watermark_predictor_type in (WATERMARK_PREDICTOR_TYPES if slice_store_type == "FILE_BACKED" else [WATERMARK_PREDICTOR_TYPES[0]])
        for query in get_queries()
    ]
