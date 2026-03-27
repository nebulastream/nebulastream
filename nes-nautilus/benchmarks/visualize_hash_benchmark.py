#!/usr/bin/env python3
"""
Visualize hash function benchmark results.

Usage:
    # Run benchmark with JSON output:
    ./hash-function-benchmark --benchmark_format=json --benchmark_out=results.json

    # Visualize:
    python3 visualize_hash_benchmark.py results.json
"""

import json
import sys
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def parse_results(filepath):
    with open(filepath) as f:
        data = json.load(f)

    results = defaultdict(lambda: defaultdict(dict))
    for bench in data["benchmarks"]:
        name = bench["name"]
        # Format: "HashName/datatype/N" where N is the parameter
        parts = name.split("/")
        hash_name = parts[0]
        data_type = parts[1]
        param = int(parts[2]) if len(parts) > 2 else 0

        results[data_type][(hash_name, param)] = {
            "time_ns": bench["real_time"],
            "cpu_ns": bench["cpu_time"],
            "iterations": bench["iterations"],
            "items_per_second": bench.get("items_per_second", 0),
            "bytes_per_second": bench.get("bytes_per_second", 0),
        }

    return results, data


def plot_throughput_by_type(results, output_prefix):
    """Plot throughput (items/sec) for each data type across hash functions."""
    hash_functions = ["MurMur3", "CRC32", "XXH3", "SipHash"]
    colors = {"MurMur3": "#2196F3", "CRC32": "#FF9800", "XXH3": "#4CAF50", "SipHash": "#E91E63"}

    data_types = [dt for dt in ["uint64", "int32", "double"] if dt in results]

    fig, axes = plt.subplots(1, len(data_types), figsize=(6 * len(data_types), 5), sharey=False)
    if len(data_types) == 1:
        axes = [axes]

    for ax, dtype in zip(axes, data_types):
        type_results = results[dtype]

        # Group by hash function, sorted by param
        for hf in hash_functions:
            params = sorted([p for (h, p) in type_results if h == hf])
            if not params:
                continue
            throughputs = []
            for p in params:
                entry = type_results[(hf, p)]
                throughputs.append(entry["items_per_second"] / 1e6)  # millions/sec

            ax.plot(params, throughputs, marker="o", label=hf, color=colors[hf], linewidth=2)

        ax.set_xlabel("Number of values")
        ax.set_ylabel("Throughput (M hashes/sec)")
        ax.set_title(f"Hash throughput — {dtype}")
        ax.set_xscale("log")
        ax.legend()
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f"{output_prefix}_throughput.png", dpi=150)
    print(f"Saved {output_prefix}_throughput.png")


def plot_time_per_hash(results, output_prefix):
    """Bar chart: nanoseconds per hash for a fixed input size."""
    hash_functions = ["MurMur3", "CRC32", "XXH3", "SipHash"]
    colors = ["#2196F3", "#FF9800", "#4CAF50", "#E91E63"]

    data_types = [dt for dt in ["uint64", "int32", "double"] if dt in results]

    # Pick the largest input size for each data type
    fig, ax = plt.subplots(figsize=(10, 6))

    x = np.arange(len(data_types))
    width = 0.18
    offsets = np.arange(len(hash_functions)) - (len(hash_functions) - 1) / 2

    for i, hf in enumerate(hash_functions):
        ns_per_hash = []
        for dtype in data_types:
            type_results = results[dtype]
            params = sorted([p for (h, p) in type_results if h == hf])
            if params:
                largest = params[-1]
                entry = type_results[(hf, largest)]
                ns_per_hash.append(entry["cpu_ns"] / largest)
            else:
                ns_per_hash.append(0)

        ax.bar(x + offsets[i] * width, ns_per_hash, width, label=hf, color=colors[i])

    ax.set_xlabel("Data Type")
    ax.set_ylabel("Time per hash (ns)")
    ax.set_title("Hash function latency comparison (largest input size)")
    ax.set_xticks(x)
    ax.set_xticklabels(data_types)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(f"{output_prefix}_latency.png", dpi=150)
    print(f"Saved {output_prefix}_latency.png")


def plot_multikey(results, output_prefix):
    """Plot multi-key hashing performance."""
    if "multikey" not in results:
        return

    hash_functions = ["MurMur3", "CRC32", "XXH3", "SipHash"]
    colors = {"MurMur3": "#2196F3", "CRC32": "#FF9800", "XXH3": "#4CAF50", "SipHash": "#E91E63"}

    type_results = results["multikey"]

    fig, ax = plt.subplots(figsize=(8, 5))

    for hf in hash_functions:
        params = sorted([p for (h, p) in type_results if h == hf])
        if not params:
            continue
        times = [type_results[(hf, p)]["cpu_ns"] for p in params]
        ax.plot(params, times, marker="s", label=hf, color=colors[hf], linewidth=2)

    ax.set_xlabel("Number of keys")
    ax.set_ylabel("Time per hash (ns)")
    ax.set_title("Multi-key hashing latency")
    ax.set_xticks(range(1, 9))
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f"{output_prefix}_multikey.png", dpi=150)
    print(f"Saved {output_prefix}_multikey.png")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <benchmark_results.json> [output_prefix]")
        sys.exit(1)

    filepath = sys.argv[1]
    output_prefix = sys.argv[2] if len(sys.argv) > 2 else "hash_benchmark"

    results, raw_data = parse_results(filepath)

    plot_throughput_by_type(results, output_prefix)
    plot_time_per_hash(results, output_prefix)
    plot_multikey(results, output_prefix)

    print("\nDone. Generated plots:")
    print(f"  {output_prefix}_throughput.png  — Throughput scaling by data type")
    print(f"  {output_prefix}_latency.png    — Per-hash latency comparison")
    print(f"  {output_prefix}_multikey.png   — Multi-key hashing performance")


if __name__ == "__main__":
    main()
