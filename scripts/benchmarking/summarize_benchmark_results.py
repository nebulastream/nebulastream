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
import re
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


# Known mode suffixes to strip from query names so all modes share the same query key
MODE_SUFFIXES = ["file", "tmpfs_cold", "tmpfs_warm", "memory", "in_place"]


def normalize_query_name(query: str) -> str:
    """Strip mode-specific suffixes so queries group across modes.
    e.g. 'YSB_file' and 'YSB_memory' both become 'YSB'."""
    for suffix in sorted(MODE_SUFFIXES, key=len, reverse=True):
        pattern = rf"[_\-]{re.escape(suffix)}$"
        stripped = re.sub(pattern, "", query)
        if stripped != query:
            return stripped
    return query


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <benchmark_results.csv>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    # Key: (threads, scheduling_label, mode, normalized_query) -> {time: [], bps: [], tps: []}
    results = defaultdict(lambda: {"time": [], "bps": [], "tps": []})
    has_scheduling = False
    has_threads = False

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            threads = row.get("threads", "4")
            sched = row.get("scheduling", "GLOBAL_QUEUE")
            ws = row.get("work_stealing", "false")
            if sched != "GLOBAL_QUEUE":
                has_scheduling = True
            sched_label = sched
            if ws == "true":
                sched_label += "+WS"
            mode = row["mode"]
            query = normalize_query_name(row["query"])
            key = (threads, sched_label, mode, query)
            results[key]["time"].append(float(row["time_seconds"]))
            results[key]["bps"].append(float(row["bytes_per_second"]))
            results[key]["tps"].append(float(row["tuples_per_second"]))

    if not results:
        print("No results collected.")
        sys.exit(0)

    queries = sorted(set(k[3] for k in results.keys()))
    thread_counts = sorted(set(k[0] for k in results.keys()), key=lambda x: int(x))
    has_threads = len(thread_counts) > 1
    sched_order = [
        "GLOBAL_QUEUE",
        "PER_THREAD_ROUND_ROBIN",
        "PER_THREAD_ROUND_ROBIN+WS",
        "PER_THREAD_SMALLEST_QUEUE",
        "PER_THREAD_SMALLEST_QUEUE+WS",
    ]
    all_scheds = set(k[1] for k in results.keys())
    scheds = [s for s in sched_order if s in all_scheds]
    # Include any remaining labels not in the predefined order
    scheds += sorted(s for s in all_scheds if s not in scheds)
    mode_order = ["file", "tmpfs_cold", "tmpfs_warm", "memory", "in_place"]
    modes = [m for m in mode_order if any(k[2] == m for k in results.keys())]

    print()
    print("=================================================================")
    print(" BenchmarkSource Mode Comparison — Summary")
    print("=================================================================")
    print()

    for query in queries:
        print(f"Query: {query}")

        # Build label
        parts = []
        if has_threads:
            parts.append("Threads")
        parts.append("Mode")
        if has_scheduling:
            parts.append("Scheduling")
        label_header = " / ".join(parts)
        col_w = 50
        header = f"  {label_header:<{col_w}} {'Avg Time':>10} {'Throughput':>16} {'Tuples':>16}  {'vs baseline':>11}"
        print(header)
        print(
            f"  {'-' * col_w} {'-' * 10} {'-' * 16} {'-' * 16}  {'-' * 11}"
        )

        for mode in modes:
            mode_baseline = None
            for tc in thread_counts:
                for sched in scheds:
                    key = (tc, sched, mode, query)
                    if key not in results:
                        continue

                    n = len(results[key]["time"])
                    avg_time = sum(results[key]["time"]) / n
                    avg_bps = sum(results[key]["bps"]) / n
                    avg_tps = sum(results[key]["tps"]) / n

                    if mode_baseline is None:
                        mode_baseline = avg_time

                    if mode_baseline > 0 and avg_time > 0:
                        speedup = mode_baseline / avg_time
                        speedup_str = (
                            "(baseline)"
                            if abs(speedup - 1.0) < 0.005
                            else f"{speedup:.2f}x"
                        )
                    else:
                        speedup_str = "n/a"

                    parts = []
                    if has_threads:
                        parts.append(f"{tc}T")
                    parts.append(mode)
                    if has_scheduling:
                        parts.append(sched)
                    label = " / ".join(parts)

                    print(
                        f"  {label:<{col_w}} {avg_time:>8.4f}s  {fmt_throughput(avg_bps):>16} {fmt_tps(avg_tps):>16}  {speedup_str:>11}"
                    )

            if mode != modes[-1]:
                print()  # visual separator between source modes

        print()

    print(f"Raw data: {csv_path}")
    print()


if __name__ == "__main__":
    main()
