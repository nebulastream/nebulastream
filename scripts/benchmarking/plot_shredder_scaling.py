# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TMP DIAGNOSTIC (revert before merge). Plots lock-free vs LOCKING SequenceShredder throughput from the
# formatter-scaling benchmark, across raw-buffer sizes at 14 threads. Regenerate the data:
#   for buf in 0.5 1 2 4 8 16 32 64 128; do
#     formatter-scaling-benchmark bench_5xUINT64_512m.csv $buf 0 11 14 lean 5 f1 <fnattr.yaml> both FastUINT64
#   done   # read the 14-thread "median | min..max" rows for each shredder mode
# Usage:  python3 plot_shredder_scaling.py [data.txt] [out.png] [normal|revx|revxy]
#   data.txt lines: "<rawBufKiB> <LOCK_FREE> <LOCKING>" (defaults to the measured run below).

import sys

# Measured: Apple M5 Max, 14-vCPU dev container, Release; SIMDCSV + FastUINT64, 5-col index + 1-field
# parse (project f1 + fnattr) -- the SELECT-key scaling workload -- 14 worker threads, 512 MiB CSV,
# SequenceShredder ring = 65536 (raised from 1024 to stop sub-page repeat storms). Median of 11 reps.
# Columns: rawBufKiB, LF median, LOCKING median, LF min, LF max, LOCKING min, LOCKING max.
# Note: with the enlarged ring, LOCK_FREE has 0 repeats at every size; LOCKING develops a severe repeat
# storm below 4 KiB (247% / 2961% / 8116% of buffers at 2 / 1 / 0.5 KiB) and collapses there.
MEASURED = [
    (0.5, 1.304, 0.037, 0.965, 1.346, 0.005, 0.419),
    (1, 2.588, 0.696, 2.333, 2.608, 0.047, 0.885),
    (2, 4.860, 1.416, 3.769, 4.958, 0.279, 1.716),
    (4, 8.733, 2.907, 7.662, 8.958, 2.336, 3.106),
    (8, 14.051, 5.906, 12.757, 14.451, 4.757, 6.344),
    (16, 19.947, 10.795, 19.200, 20.985, 8.495, 11.835),
    (32, 26.485, 17.511, 24.853, 27.497, 14.457, 19.901),
    (64, 30.683, 28.048, 30.268, 31.375, 25.165, 29.664),
    (128, 32.478, 32.228, 30.237, 33.515, 30.031, 33.785),
]


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 3:
                try:
                    rows.append(tuple(float(p) for p in parts[:3]))
                except ValueError:
                    continue
    return rows


def main():
    data_path = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "-" else None
    out_path = sys.argv[2] if len(sys.argv) > 2 else "scripts/benchmarking/shredder_scaling.png"
    orient = sys.argv[3] if len(sys.argv) > 3 else "normal"  # normal | revx | revxy
    rows = load(data_path) if data_path else MEASURED

    bufs = [r[0] for r in rows]
    lf = [r[1] for r in rows]
    lk = [r[2] for r in rows]
    ratio = [a / b if b > 0 else float("inf") for a, b in zip(lf, lk)]

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 5.5))

    ax.plot(bufs, lf, "-o", color="#1b7837", lw=2.4, ms=7, label="LOCK_FREE shredder", zorder=3)
    ax.plot(bufs, lk, "--s", color="#b2182b", lw=2.4, ms=7, label="LOCKING shredder", zorder=3)

    for x, a, r in zip(bufs, lf, ratio):
        if r >= 1.15:
            ax.annotate(
                f"{r:.1f}x", xy=(x, a), xytext=(0, 10), textcoords="offset points",
                ha="center", fontsize=10, fontweight="bold", color="#1b7837",
            )

    ax.set_xscale("log", base=2)
    ax.set_xticks(bufs)
    ax.set_xticklabels([(f"{b:g}" if b >= 1 else "0.5") for b in bufs])
    ax.set_xlabel("raw buffer size (KiB, log scale)  -  4 KiB = page size = engine default", fontsize=11)
    ax.set_ylabel("formatter throughput (GB/s of input)", fontsize=11)
    ax.set_title(
        "Lock-free vs LOCKING SequenceShredder (ring=65536)\n"
        "SIMDCSV + FastUINT64, SELECT-key, 14 threads, median of 11 (log y: vertical gap = speedup)\n"
        "Apple M5 Max, 14-vCPU container",
        fontsize=11.5,
    )
    ax.set_yscale("log")
    ax.set_ylim(0.03, 38)
    ax.set_yticks([0.1, 1, 10, 30])
    ax.set_yticklabels(["0.1", "1", "10", "30"])
    ax.minorticks_off()
    ax.grid(True, which="major", ls=":", alpha=0.4)
    ax.legend(loc="best", fontsize=10, framealpha=0.95)

    if orient in ("revx", "revxy"):
        ax.invert_xaxis()
    if orient == "revxy":
        ax.invert_yaxis()

    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    print(f"wrote {out_path}")
    for x, a, b, r in zip(bufs, lf, lk, ratio):
        print(f"  {x:>5} KiB: LF={a:6.3f}  LOCK={b:6.3f}  ratio={r:.2f}x")


if __name__ == "__main__":
    main()
