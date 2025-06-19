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
    keys_to_remove = {key for key in all_failed_runs_dicts[0] if all(d[key] == all_failed_runs_dicts[0][key] for d in all_failed_runs_dicts)}

    # Remove those keys from each dictionary
    print([{k: v for k, v in d.items() if k not in keys_to_remove} for d in all_failed_runs_dicts])
