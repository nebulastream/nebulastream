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

"""
Split a newline-delimited CSV into N line-aligned slices of roughly equal size.

Used by the Phase-0 producer-bottleneck experiment (see
producer-bottleneck-findings.md): each slice becomes its own File source, so a
UNION of N slices runs with N independent producer threads while the total input
stays a fixed 5 GB (keeping run times comparable across producer counts).

Boundaries are found by seeking to i*size/N and advancing to the next newline,
then byte ranges are copied out -- one pass, no line is ever split. Idempotent:
if the slices already exist and their sizes sum to the input size, it's a no-op.

CLI:  python3 slice_csv.py <input.csv> <N> <out_dir>
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

COPY_CHUNK = 16 << 20  # 16 MiB


def slice_paths(out_dir: Path, n: int) -> list[Path]:
    return [out_dir / f"slice_{i}_of_{n}.csv" for i in range(n)]


def slice_file(input_path: Path, n: int, out_dir: Path) -> list[Path]:
    """Split input_path into n line-aligned slices in out_dir; return their paths."""
    input_path = Path(input_path)
    out_dir = Path(out_dir)
    size = input_path.stat().st_size
    paths = slice_paths(out_dir, n)

    # Idempotent: reuse if all slices exist and their sizes sum to the input size.
    if all(p.exists() for p in paths) and sum(p.stat().st_size for p in paths) == size:
        return paths

    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Find n-1 interior boundaries at the next newline after each i*size/n cut.
    boundaries = [0]
    with input_path.open("rb") as f:
        for i in range(1, n):
            f.seek((i * size) // n)
            f.readline()  # advance past the partial line to the start of the next
            boundaries.append(f.tell())
    boundaries.append(size)

    # 2) Copy each [start, end) byte range into its slice.
    with input_path.open("rb") as f:
        for i, p in enumerate(paths):
            start, end = boundaries[i], boundaries[i + 1]
            f.seek(start)
            remaining = end - start
            with p.open("wb") as out:
                while remaining > 0:
                    chunk = f.read(min(COPY_CHUNK, remaining))
                    if not chunk:
                        break
                    out.write(chunk)
                    remaining -= len(chunk)
    return paths


def main() -> None:
    if len(sys.argv) != 4:
        sys.exit("usage: slice_csv.py <input.csv> <N> <out_dir>")
    input_path, n, out_dir = Path(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3])
    paths = slice_file(input_path, n, out_dir)
    total = sum(p.stat().st_size for p in paths)
    print(f"{n} slices of {input_path} ({total} bytes total, input {input_path.stat().st_size}):")
    for p in paths:
        print(f"  {p}  ({p.stat().st_size} bytes)")
    if total != input_path.stat().st_size:
        sys.exit(f"ERROR: slice sizes sum to {total}, expected {input_path.stat().st_size}")


if __name__ == "__main__":
    main()
