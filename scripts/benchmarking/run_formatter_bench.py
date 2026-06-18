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
Driver for the end-to-end input-formatter throughput benchmark
(nes-systests/benchmark/FormatterThroughput.test, on the 5 GB CSV).

Runs `systest -b` once PER QUERY (`-t file:N`) so that a query which fails at
runtime (e.g. projChk floods the sink and exhausts the buffer pool at high
thread counts) does not shift the result mapping of the others. Aggregates the
per-query throughput from each run's BenchmarkResults.json into median MB/s per
(query shape, formatter, thread count).

The .test holds 6 queries in this file order:
  1 projChk csv   2 projChk simd   (single-field projection -> Checksum sink)
  3 projVoid csv  4 projVoid simd  (single-field projection -> Void sink: formatter ceiling)
  5 agg csv       6 agg simd       (tumbling-window aggregation reference)

Usage (from repo root):
  python3 scripts/benchmarking/run_formatter_bench.py --reps 5 --threads 1 4 8
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HELPER = REPO / ".claude/skills/nes-build/in-docker.sh"
BUILD_DIR = "cmake-build-release-piparse"
TEST = "{repo}/nes-systests/benchmark/FormatterThroughput.test"
DATA = "{repo}/nes-systests/testdata"
WORKDIR_TOKEN = "{build}/fmt-bench-wd"
RESULTS_JSON = REPO / BUILD_DIR / "fmt-bench-wd" / "BenchmarkResults.json"
# Use 64 KiB operator buffers instead of the 4 KiB default: the 4 KiB default cripples high-output
# queries (per-buffer/per-task overhead + far more buffer objects). At 64 KiB the default 32768-buffer
# pool already holds ~268 M tuples (vs only ~16.7 M at 4 KiB, which is why the all-rows projection
# exhausted the pool), so no pool bump is needed -- and bumping it here would OOM (1M * 64KiB = 64 GB).
OPERATOR_BUFFER_SIZE = 65536

# query number (1-indexed, .test file order) -> (shape, formatter)
QUERIES = [
    (1, "projChk", "csv"),
    (2, "projChk", "simd"),
    (3, "projVoid", "csv"),
    (4, "projVoid", "simd"),
    (5, "agg", "csv"),
    (6, "agg", "simd"),
]
SHAPES = ["projChk", "projVoid", "agg"]


def run_one(threads: int, qnum: int) -> float | None:
    """Run a single query under -b; return its MB/s, or None if it failed to produce a result."""
    if RESULTS_JSON.exists():
        RESULTS_JSON.unlink()
    cmd = [
        str(HELPER),
        "{build}/nes-systests/systest/systest",
        "-t", f"{TEST}:{qnum}",
        "--data", DATA,
        "--workingDir", WORKDIR_TOKEN,
        "-b",
        "--",
        f"--worker.query_engine.number_of_worker_threads={threads}",
        f"--worker.default_query_execution.operator_buffer_size={OPERATOR_BUFFER_SIZE}",
    ]
    env = {**os.environ, "NES_BUILD_DIR": BUILD_DIR}
    subprocess.run(cmd, env=env, cwd=REPO, capture_output=True, text=True)
    if not RESULTS_JSON.exists():
        return None
    try:
        data = json.loads(RESULTS_JSON.read_text())
    except json.JSONDecodeError:
        return None
    # A query that failed at runtime is skipped in the results -> json is `null` or `[]`.
    if not isinstance(data, list) or len(data) != 1:
        return None
    bps = data[0]["bytesPerSecond"]
    # A failed query that still got serialized would have bogus/NaN throughput; guard.
    return bps / 1e6 if bps and bps == bps and bps > 0 else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--threads", type=int, nargs="+", default=[1, 4, 8])
    ap.add_argument("--warmup", type=int, default=1)
    args = ap.parse_args()

    for threads in args.threads:
        samples: dict[tuple, list[float]] = {(s, f): [] for s in SHAPES for f in ("csv", "simd")}
        fails: dict[tuple, int] = {(s, f): 0 for s in SHAPES for f in ("csv", "simd")}
        for rep in range(args.reps + args.warmup):
            for qnum, shape, fmt in QUERIES:
                mbps = run_one(threads, qnum)
                if rep < args.warmup:
                    continue
                if mbps is None:
                    fails[(shape, fmt)] += 1
                else:
                    samples[(shape, fmt)].append(mbps)
            tag = "warmup" if rep < args.warmup else f"rep{rep - args.warmup + 1}"
            print(f"  [threads={threads}] {tag} done", flush=True)

        print(f"\n## threads = {threads}  ({args.reps} reps, median MB/s; '-' = all reps failed)\n")
        print("| query    | CSV MB/s | SIMDCSV MB/s | SIMD/CSV |")
        print("|----------|---------:|-------------:|---------:|")
        for shape in SHAPES:
            cs, ss = samples[(shape, "csv")], samples[(shape, "simd")]
            csv = statistics.median(cs) if cs else None
            simd = statistics.median(ss) if ss else None
            ratio = f"{simd / csv:7.3f}x" if (csv and simd) else "      -"
            cstr = f"{csv:8.1f}" if csv else f"{'fail(' + str(fails[(shape, 'csv')]) + ')':>8}"
            sstr = f"{simd:12.1f}" if simd else f"{'fail(' + str(fails[(shape, 'simd')]) + ')':>12}"
            print(f"| {shape:8} | {cstr} | {sstr} | {ratio} |")
        print("\nspread (min..max, n):")
        for shape in SHAPES:
            for fmt in ("csv", "simd"):
                xs = samples[(shape, fmt)]
                if xs:
                    print(f"  {shape:8} {fmt:4}: {min(xs):8.1f} .. {max(xs):8.1f}  (n={len(xs)}, fail={fails[(shape, fmt)]})")
                else:
                    print(f"  {shape:8} {fmt:4}: all {fails[(shape, fmt)]} runs failed")


if __name__ == "__main__":
    main()
