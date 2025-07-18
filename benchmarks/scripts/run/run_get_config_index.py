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


import BenchmarkConfig


if __name__ == "__main__":
    configs = BenchmarkConfig.create_batch_size_benchmark_configs()
    data = [obj.to_dict() for obj in configs]

    target = {
        "slice_store_type": "DEFAULT",
        "timestamp_increment": 1000,
        "batch_size": 1,
        "query": "SELECT * FROM (SELECT * FROM tcp_source) INNER JOIN (SELECT * FROM tcp_source2) ON id = id2 WINDOW SLIDING (timestamp, size 1000000000 ms, advance by 1000000 ms) INTO csv_sink;"
    }

    indices = [i for i, d in enumerate(data) if all(d[k] == v for k, v in target.items())]

    if indices:
        print(f"First match found at index {indices[0]} and number of matches is {len(indices)}")
    else:
        print("No match found")
