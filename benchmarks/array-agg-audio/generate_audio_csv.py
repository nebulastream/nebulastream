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

"""Generate the deterministic, file-backed 44.1 kHz audio ARRAY_AGG benchmark input."""

from argparse import ArgumentParser
from pathlib import Path

import numpy as np


ROW_BYTES = 42
SAMPLE_RATE = 44_100


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("audio-44khz-1gb.csv"))
    parser.add_argument("--size-bytes", type=int, default=1_000_000_000)
    parser.add_argument("--seed", type=int, default=0x4E4553)
    parser.add_argument("--chunk-rows", type=int, default=250_000)
    args = parser.parse_args()

    number_of_rows = (args.size_bytes + ROW_BYTES - 1) // ROW_BYTES
    rng = np.random.default_rng(args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb", buffering=16 * 1024 * 1024) as output:
        for first_row in range(0, number_of_rows, args.chunk_rows):
            rows = min(args.chunk_rows, number_of_rows - first_row)
            indexes = np.arange(first_row, first_row + rows, dtype=np.uint64)
            timestamps = (indexes // SAMPLE_RATE) * 1_000_000_000
            timestamps += ((indexes % SAMPLE_RATE) * 1_000_000_000) // SAMPLE_RATE
            samples = rng.uniform(-1.0, 1.0, size=rows)
            np.savetxt(output, np.column_stack((samples, timestamps)), fmt="%+021.18f,%019.0f")

    actual_size = args.output.stat().st_size
    expected_size = number_of_rows * ROW_BYTES
    if actual_size != expected_size:
        raise RuntimeError(f"unexpected output size: {actual_size} bytes instead of {expected_size}")
    print(f"{args.output}: {number_of_rows} samples, {actual_size} bytes, {number_of_rows / SAMPLE_RATE:.3f} seconds")


if __name__ == "__main__":
    main()
