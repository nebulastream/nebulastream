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

"""Plots the throughput timeline of nes-reference-interpreted-to-compiled-prototype.

Usage: plot_interpreted_compiled_throughput.py THROUGHPUT.csv OUTPUT.png   (requires matplotlib)
"""

import csv
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

COLORS = {"interpreted": "#2a78d6", "compiled": "#eb6834"}
SURFACE, INK, INK_SECONDARY, MUTED, GRID, BASELINE, SWITCH = (
    "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#f0efec")
SAMPLE_SECONDS = 0.25


def runs(rows, key):
    """Splits rows into maximal runs with the same key value."""
    current = []
    for row in rows:
        if current and key(row) != key(current[-1]):
            yield current
            current = []
        current.append(row)
    if current:
        yield current


def main(path, output):
    with open(path) as file:
        rows = [
            {"time": float(r["time_s"]), "rate": float(r["rows_per_s"]), "running": r["running"], "switching": r["switching"] == "1"}
            for r in csv.DictReader(file)
        ]

    fig, ax = plt.subplots(figsize=(11, 4.8), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    for switch in (run for run in runs(rows, lambda r: r["switching"]) if run[0]["switching"]):
        start, end = switch[0]["time"] - SAMPLE_SECONDS, switch[-1]["time"]
        ax.axvspan(start, end, color=SWITCH, zorder=0, linewidth=0)
        target = "compiled" if switch[0]["running"] == "interpreted" else "interpreted"
        ax.annotate(f"switch to {target}\n{end - start:.1f} s", xy=((start + end) / 2, 1.0), xycoords=("data", "axes fraction"),
                    xytext=(0, -4), textcoords="offset points", ha="center", va="top", fontsize=8, color=INK_SECONDARY)

    # Each segment includes the next segment's first sample, so the line stays continuous where the running version changes.
    segments = list(runs(rows, lambda r: r["running"]))
    for index, segment in enumerate(segments):
        points = segment + segments[index + 1][:1] if index + 1 < len(segments) else segment
        points = [p for p in points if p["rate"] > 0]
        ax.plot([p["time"] for p in points], [p["rate"] for p in points], color=COLORS[segment[0]["running"]], linewidth=2,
                solid_capstyle="round", zorder=3)

    # Direct label per steady phase: its median, excluding switches and the catch-up after a switch to the faster version.
    for phase in (run for run in runs(rows, lambda r: (r["running"], r["switching"])) if not run[0]["switching"]):
        median = statistics.median(p["rate"] for p in phase)
        middle = (phase[0]["time"] + phase[-1]["time"]) / 2
        ax.annotate(f"{median / 1000:,.0f}k rows/s", xy=(middle, median), xytext=(0, 8), textcoords="offset points", ha="center",
                    fontsize=9, color=INK)

    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value / 1000:,.0f}k" if value >= 1000 else f"{value:,.0f}"))
    ax.set_ylim(bottom=max(1000, min(p["rate"] for p in rows if p["rate"] > 0) / 2), top=max(p["rate"] for p in rows) * 3)
    ax.set_xlim(0, rows[-1]["time"])
    ax.set_xlabel("time since first result (s)", color=INK_SECONDARY)
    ax.set_ylabel("input throughput (rows/s, log scale)", color=INK_SECONDARY)
    ax.grid(axis="y", which="major", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)
    ax.tick_params(colors=MUTED, which="both")
    ax.tick_params(axis="y", length=0)

    handles = [plt.Line2D([], [], color=COLORS[name], linewidth=2, label=name) for name in ("interpreted", "compiled")]
    handles.append(plt.Rectangle((0, 0), 1, 1, color=SWITCH, label="switch (JIT + state transfer)"))
    ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(0, -0.13), ncol=3, frameon=False, fontsize=9, labelcolor=INK_SECONDARY)
    ax.set_title("Window query throughput, switching between the Nautilus interpreter and the JIT", loc="left", color=INK, fontsize=12,
                 pad=14)
    fig.tight_layout()
    fig.savefig(output, facecolor=SURFACE)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
