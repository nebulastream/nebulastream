#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0

"""Plot benchmark results as a line chart. Outputs a PNG file."""

import csv
import sys
from collections import defaultdict

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    print("ERROR: matplotlib is required. Install with: pip3 install matplotlib", file=sys.stderr)
    sys.exit(1)

# Known mode suffixes to strip from query names
MODE_SUFFIXES = ["file", "tmpfs_cold", "tmpfs_warm", "memory", "in_place"]

def normalize_query(query):
    import re
    for suffix in sorted(MODE_SUFFIXES, key=len, reverse=True):
        pattern = rf"[_\-]{re.escape(suffix)}$"
        stripped = re.sub(pattern, "", query)
        if stripped != query:
            return stripped
    return query


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <benchmark_results.csv> [output.png]", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else "benchmark_chart.png"

    # Key: (threads, sched_label, mode) -> {bps: []}
    results = defaultdict(lambda: {"bps": [], "tps": []})

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            threads = int(row.get("threads", "4"))
            sched = row.get("scheduling", "GLOBAL_QUEUE")
            ws = row.get("work_stealing", "false")
            sched_label = sched
            if ws == "true":
                sched_label += "+WS"
            mode = row["mode"]
            key = (threads, sched_label, mode)
            results[key]["bps"].append(float(row["bytes_per_second"]))
            results[key]["tps"].append(float(row["tuples_per_second"]))

    if not results:
        print("No results.")
        sys.exit(1)

    thread_counts = sorted(set(k[0] for k in results.keys()))
    scheds = sorted(set(k[1] for k in results.keys()))
    modes = sorted(set(k[2] for k in results.keys()))

    has_threads = len(thread_counts) > 1

    # Build series: one line per (scheduling, mode) combination
    series = {}
    for sched in scheds:
        for mode in modes:
            label_parts = []
            if len(modes) > 1:
                label_parts.append(mode)
            label_parts.append(sched.replace("PER_THREAD_", "PT_"))
            label = " / ".join(label_parts)

            xs = []
            ys = []
            for tc in thread_counts:
                key = (tc, sched, mode)
                if key in results:
                    avg_bps = sum(results[key]["bps"]) / len(results[key]["bps"])
                    xs.append(tc)
                    ys.append(avg_bps / (1024 * 1024))  # Convert to MB/s
            if xs:
                series[label] = (xs, ys)

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))

    markers = ["o", "s", "^", "D", "v", "P", "X", "*"]
    colors = plt.cm.tab10.colors

    # Use evenly spaced positions so the x-axis is linear, not log-like
    tick_positions = list(range(len(thread_counts)))
    tick_labels = [str(t) for t in thread_counts]
    tc_to_pos = {tc: pos for tc, pos in zip(thread_counts, tick_positions)}

    for i, (label, (xs, ys)) in enumerate(series.items()):
        xpos = [tc_to_pos[x] for x in xs]
        ax.plot(xpos, ys,
                marker=markers[i % len(markers)],
                color=colors[i % len(colors)],
                linewidth=2, markersize=8,
                label=label)

    if has_threads:
        ax.set_xlabel("Worker Threads", fontsize=13)
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels)
    else:
        ax.set_xlabel("Configuration", fontsize=13)

    ax.set_ylabel("Throughput (MB/s)", fontsize=13)
    ax.set_title("BenchmarkSource — Scheduling Strategy vs Thread Count", fontsize=14)
    ax.legend(loc="best", fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.0f"))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"Chart saved to: {out_path}")


if __name__ == "__main__":
    main()
