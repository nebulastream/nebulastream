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
Thread-scaling driver for the single-field projection WITHOUT emit.

Runs only the projVoid CSV query (FormatterThroughput.test:3 -- SELECT key FROM
csv5 INTO csv5_projVoid) under `systest -b`, with the NES_DISABLE_EMIT diagnostic
gate enabled so pipelines drop their emitted buffers (the sink sees nothing).
That isolates source + scan/parse + projection from the per-buffer emit/sink-task
cost. The formatter still parses every row; only the sink-bound emit is dropped.
The Void sink emits an empty result either way, so the result check still passes
and the timing serializes to BenchmarkResults.json.

Buffers are 128 KiB. Throughput denominator is the input file size in bytes, so
MB/s is directly comparable across thread counts. Prints median MB/s per thread
count and the speedup relative to the 1-thread median.

Usage (from repo root):
  python3 scripts/benchmarking/run_proj_noemit_bench.py --reps 5 --threads 1 2 3 4 5 6 7 8
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HELPER = REPO / ".claude/skills/nes-build/in-docker.sh"
BUILD_DIR = "cmake-build-release-piparse"
TEST = "{repo}/nes-systests/benchmark/FormatterThroughput.test"
DATA = "{repo}/nes-systests/testdata"
WORKDIR_TOKEN = "{build}/fmt-bench-wd"
RESULTS_JSON = REPO / BUILD_DIR / "fmt-bench-wd" / "BenchmarkResults.json"
# csv5_projVoid: SELECT key FROM csv5 (scalar CSV formatter) INTO a Void sink.
QUERY_NUM = 3
# 128 KiB operator buffers (vs the 4 KiB default / 64 KiB used by the full-matrix driver).
OPERATOR_BUFFER_SIZE = 128 * 1024


def run_one(threads: int) -> float | None:
    """Run the projVoid CSV query once with emit disabled; return MB/s or None on failure."""
    if RESULTS_JSON.exists():
        RESULTS_JSON.unlink()
    cmd = [
        str(HELPER),
        "env", "NES_DISABLE_EMIT=1",  # diagnostic gate: drop sink-bound emit, still parse every row
        "{build}/nes-systests/systest/systest",
        "-t", f"{TEST}:{QUERY_NUM}",
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
    if not isinstance(data, list) or len(data) != 1:
        return None
    bps = data[0]["bytesPerSecond"]
    return bps / 1e6 if bps and bps == bps and bps > 0 else None


def fmt_throughput(mbps: float) -> str:
    """Render MB/s below 1000, GB/s above -- matching the requested table style."""
    return f"{mbps:6.0f} MB/s" if mbps < 1000 else f"{mbps / 1000:5.2f} GB/s"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--threads", type=int, nargs="+", default=[1, 2, 3, 4, 5, 6, 7, 8])
    ap.add_argument("--warmup", type=int, default=1)
    args = ap.parse_args()

    medians: dict[int, float | None] = {}
    spreads: dict[int, tuple[float, float, int]] = {}
    for threads in args.threads:
        samples: list[float] = []
        fails = 0
        for rep in range(args.reps + args.warmup):
            mbps = run_one(threads)
            if rep < args.warmup:
                tag = "warmup"
            else:
                tag = f"rep{rep - args.warmup + 1}"
                if mbps is None:
                    fails += 1
                else:
                    samples.append(mbps)
            shown = f"{mbps:8.1f} MB/s" if mbps else "    FAILED"
            print(f"  [threads={threads}] {tag}: {shown}", flush=True)
        medians[threads] = statistics.median(samples) if samples else None
        if samples:
            spreads[threads] = (min(samples), max(samples), len(samples))
        else:
            spreads[threads] = (0.0, 0.0, 0)
        if fails:
            print(f"  [threads={threads}] {fails} rep(s) FAILED", flush=True)

    base = medians.get(args.threads[0])
    print("\nproj-no-emit (csv5_projVoid, NES_DISABLE_EMIT=1, 128 KiB buffers, median of "
          f"{args.reps} reps)\n")
    print("┌─────────┬──────────────┬─────────┐")
    print("│ threads │ proj-no-emit │ speedup │")
    print("├─────────┼──────────────┼─────────┤")
    for threads in args.threads:
        m = medians[threads]
        if m is None:
            print(f"│ {threads:7d} │       FAILED │       - │")
            continue
        speed = f"{m / base:.1f}×" if base else "-"
        print(f"│ {threads:7d} │ {fmt_throughput(m):>12} │ {speed:>6} │")
    print("└─────────┴──────────────┴─────────┘")

    print("\nspread (min..max MB/s, n):")
    for threads in args.threads:
        lo, hi, n = spreads[threads]
        print(f"  threads={threads}: {lo:8.1f} .. {hi:8.1f}  (n={n})")


if __name__ == "__main__":
    main()
