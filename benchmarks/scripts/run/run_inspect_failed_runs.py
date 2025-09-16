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


import os
import yaml
from collections import Counter


# Compilation for misc.
ALL_FAILED_RUNS = ['.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347484',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347517',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347550',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347617',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347650',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347683',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347716',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347749',
                   '.cache/benchmarks/2025-06-19_11-56-48/SpillingBenchmarks_1750347782']
BENCHMARK_CONFIG_FILE = "benchmark_config.yaml"


if __name__ == "__main__":
    all_failed_runs_dicts = [yaml.safe_load(open(os.path.join(failed_run, BENCHMARK_CONFIG_FILE), 'r')) for failed_run
                             in ALL_FAILED_RUNS]
    # Find keys where all values are the same
    identical_keys = {key for key in all_failed_runs_dicts[0] if all(d[key] == all_failed_runs_dicts[0][key] for d in all_failed_runs_dicts)}
    identical_keys_values = {key: Counter(d[key] for d in all_failed_runs_dicts) for key in all_failed_runs_dicts[0] if key in identical_keys}
    print(f"Identical keys and values:\n{identical_keys_values}")

    # Collect unique values for keys that vary between dictionaries
    varying_keys_values = {key: Counter(d[key] for d in all_failed_runs_dicts) for key in all_failed_runs_dicts[0] if key not in identical_keys}
    print(f"Varying keys and values:\n{varying_keys_values}")

    # Remove those keys from each dictionary
    # print([{k: v for k, v in d.items() if k not in identical_keys} for d in all_failed_runs_dicts])
