#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Reads a benchmark_results.csv produced by run_benchmark_source_modes.sh and
prints a human-readable summary table comparing modes.
"""

import csv
import sys
from collections import defaultdict


def fmt_throughput(bps: float) -> str:
    for unit in ["B/s", "KB/s", "MB/s", "GB/s", "TB/s"]:
        if abs(bps) < 1024:
            return f"{bps:8.2f} {unit}"
        bps /= 1024
    return f"{bps:8.2f} PB/s"


def fmt_tps(tps: float) -> str:
    for unit in ["tup/s", "Ktup/s", "Mtup/s"]:
        if abs(tps) < 1000:
            return f"{tps:8.2f} {unit}"
        tps /= 1000
    return f"{tps:8.2f} Gtup/s"


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <benchmark_results.csv>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    results = defaultdict(lambda: {"time": [], "bps": [], "tps": []})

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["mode"], row["query"])
            results[key]["time"].append(float(row["time_seconds"]))
            results[key]["bps"].append(float(row["bytes_per_second"]))
            results[key]["tps"].append(float(row["tuples_per_second"]))

    if not results:
        print("No results collected.")
        sys.exit(0)

    queries = sorted(set(k[1] for k in results.keys()))
    # Order modes from slowest (disk) to fastest (zero-copy)
    mode_order = ["file", "tmpfs_cold", "tmpfs_warm", "memory", "in_place"]
    modes = [m for m in mode_order if any((m, q) in results for q in queries)]

    print()
    print("=================================================================")
    print(" BenchmarkSource Mode Comparison — Summary")
    print("=================================================================")
    print()

    for query in queries:
        print(f"Query: {query}")
        header = f"  {'Mode':<15} {'Avg Time':>10} {'Throughput':>16} {'Tuples':>16}  {'vs File':>10}"
        print(header)
        print(f"  {'-' * 15} {'-' * 10} {'-' * 16} {'-' * 16}  {'-' * 10}")

        baseline_time = None
        for mode in modes:
            key = (mode, query)
            if key not in results:
                continue

            n = len(results[key]["time"])
            avg_time = sum(results[key]["time"]) / n
            avg_bps = sum(results[key]["bps"]) / n
            avg_tps = sum(results[key]["tps"]) / n

            if baseline_time is None:
                baseline_time = avg_time

            if baseline_time > 0 and avg_time > 0:
                speedup = baseline_time / avg_time
                speedup_str = (
                    "(baseline)" if abs(speedup - 1.0) < 0.005 else f"{speedup:.2f}x"
                )
            else:
                speedup_str = "n/a"

            print(
                f"  {mode:<15} {avg_time:>8.4f}s  {fmt_throughput(avg_bps):>16} {fmt_tps(avg_tps):>16}  {speedup_str:>10}"
            )

        print()

    print(f"Raw data: {csv_path}")
    print()


if __name__ == "__main__":
    main()
