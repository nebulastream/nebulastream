# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Writes the input of benchmark/PythonUdf.test: `id,s` rows with a running id and a 16-letter lowercase string.

    python3 generate_udf_bench_input.py /dev/shm/nes-udf-bench/udf/udf_bench.csv --rows 30000000
    python3 generate_udf_bench_input.py /dev/shm/nes-udf-bench/udf/udf_bench_3m.csv --rows 3000000
    python3 generate_udf_bench_input.py /dev/shm/nes-udf-bench/udf/udf_bench_1m.csv --rows 1000000

Then run the benchmark with `--data /dev/shm/nes-udf-bench`.
"""

import argparse
import random
import string

CHUNK = 1_000_000


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("output")
    parser.add_argument("--rows", type=int, default=30_000_000)
    parser.add_argument("--seed", type=int, default=1)
    arguments = parser.parse_args()

    generator = random.Random(arguments.seed)
    strings = ["".join(generator.choices(string.ascii_lowercase, k=16)) for _ in range(100_000)]
    with open(arguments.output, "w", encoding="ascii") as output:
        for start in range(0, arguments.rows, CHUNK):
            end = min(start + CHUNK, arguments.rows)
            output.write("".join(f"{row},{strings[row % len(strings)]}\n" for row in range(start, end)))


if __name__ == "__main__":
    main()
