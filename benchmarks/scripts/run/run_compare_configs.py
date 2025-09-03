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


from collections import Counter
import BenchmarkConfig


if __name__ == "__main__":
    configs_1 = BenchmarkConfig.create_benchmark_configs()
    configs_2 = BenchmarkConfig.create_benchmark_configs()

    # Convert configurations to sets of frozensets for comparison
    set_1 = set(frozenset(config.to_dict().items()) for config in configs_1)
    set_2 = set(frozenset(config.to_dict().items()) for config in configs_2)

    # Find the symmetric difference between the two sets
    diff = set_1.symmetric_difference(set_2)

    # Check if the sets are equal
    if not diff:
        print(f"The configurations are identical. Number of configs: {len(configs_1)}")
        keys_values_count = {key: Counter(c.to_dict()[key] for c in configs_1) for key in configs_1[0].to_dict()}
        print(f"Keys and values:\n{keys_values_count}")
    else:
        print("The configurations are not identical. Differences found:")
        for elem in diff:
            # Convert the frozenset back to a dictionary for printing
            elem_dict = dict(elem)
            print(elem_dict)
