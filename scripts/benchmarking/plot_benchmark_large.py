#!/usr/bin/env python3
import csv
import sys
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

csv_path = sys.argv[1] if len(sys.argv) > 1 else "benchmark_results_remote.csv"
out_path = sys.argv[2] if len(sys.argv) > 2 else "benchmark_chart_large.png"

results = defaultdict(lambda: {"bps": []})
with open(csv_path) as f:
    for row in csv.DictReader(f):
        threads = int(row.get("threads", "4"))
        sched = row.get("scheduling", "GLOBAL_QUEUE")
        ws = row.get("work_stealing", "false")
        label = sched
        if ws == "true":
            label += "+WS"
        key = (threads, label)
        results[key]["bps"].append(float(row["bytes_per_second"]))

thread_counts = sorted(set(k[0] for k in results.keys()))
labels_order = [
    "GLOBAL_QUEUE",
    "PER_THREAD_ROUND_ROBIN",
    "PER_THREAD_ROUND_ROBIN+WS",
    "PER_THREAD_SMALLEST_QUEUE",
    "PER_THREAD_SMALLEST_QUEUE+WS",
]
labels = [l for l in labels_order if any(k[1] == l for k in results.keys())]

display_names = {
    "GLOBAL_QUEUE": "Global Queue",
    "PER_THREAD_ROUND_ROBIN": "Per-Thread Round Robin",
    "PER_THREAD_ROUND_ROBIN+WS": "Per-Thread Round Robin + Work Stealing",
    "PER_THREAD_SMALLEST_QUEUE": "Per-Thread Smallest Queue",
    "PER_THREAD_SMALLEST_QUEUE+WS": "Per-Thread Smallest Queue + Work Stealing",
}

colors = {
    "GLOBAL_QUEUE": "#2196F3",
    "PER_THREAD_ROUND_ROBIN": "#FF9800",
    "PER_THREAD_ROUND_ROBIN+WS": "#4CAF50",
    "PER_THREAD_SMALLEST_QUEUE": "#F44336",
    "PER_THREAD_SMALLEST_QUEUE+WS": "#9C27B0",
}
markers = {
    "GLOBAL_QUEUE": "o",
    "PER_THREAD_ROUND_ROBIN": "s",
    "PER_THREAD_ROUND_ROBIN+WS": "^",
    "PER_THREAD_SMALLEST_QUEUE": "D",
    "PER_THREAD_SMALLEST_QUEUE+WS": "v",
}

fig, ax = plt.subplots(figsize=(14, 8))

for label in labels:
    xs, ys = [], []
    for tc in thread_counts:
        key = (tc, label)
        if key in results:
            avg = sum(results[key]["bps"]) / len(results[key]["bps"])
            xs.append(tc)
            ys.append(avg / (1024 * 1024))
    ax.plot(xs, ys,
            marker=markers.get(label, "o"),
            color=colors.get(label, "#666"),
            linewidth=2.5, markersize=10,
            label=display_names.get(label, label))

ax.set_xlabel("Worker Threads", fontsize=14)
ax.set_ylabel("Throughput (MB/s)", fontsize=14)
ax.set_title("Scheduling Strategy Comparison — YSB 10k (958 MB, memory mode)\n64-core server, 3 iterations averaged",
             fontsize=15, fontweight='bold')
ax.set_xticks(thread_counts)
ax.set_xticklabels([str(t) for t in thread_counts], fontsize=12)
ax.tick_params(axis='y', labelsize=12)
ax.legend(loc="upper left", fontsize=11, framealpha=0.9)
ax.grid(True, alpha=0.3, linestyle='--')
ax.set_ylim(bottom=0)
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.0f"))

plt.tight_layout()
plt.savefig(out_path, dpi=150)
print(f"Chart saved to: {out_path}")
