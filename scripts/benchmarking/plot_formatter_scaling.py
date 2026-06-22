# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TMP DIAGNOSTIC (revert before merge). Plots formatter throughput scaling 1..14 threads for the
# SELECT-key-equivalent workload (project f1 + fnattr). Produces two PNGs: an absolute-throughput
# "scalability" plot and a linear-scale speedup-vs-ideal plot ("how close to linear"). Regenerate data
# on the 5 GiB file (the 512 MiB file made the 14-thread window ~21 ms -- too short, noisy):
#   formatter-scaling-benchmark bench_5xUINT64_5g.csv 128 0 5 1,2,...,14 mttq 5 f1 <fnattr.yaml>
#   (take the LOCK_FREE "threads | GB/s" rows)
# Usage: python3 plot_formatter_scaling.py [data.txt] [out_prefix]
#   data.txt lines: "<threads> <GB/s>" (defaults to the measured run below).

import sys

# Measured: Apple M5 Max, 14-vCPU dev container, Release; SIMDCSV, 5-col index + 1-field parse
# (project f1 + fnattr), 128 KiB buffers, 5 GiB file (bench_5xUINT64_5g.csv), pre-loaded buffers (no IO),
# median of 5 reps + 1 discarded warmup, drain-only timing (thread spawn + eps->stop() excluded).
# WITH the single-thread optimizations: FastUINT64 parser + NEON vpaddq-bulk movemask index kernel.
# Whole curve ~+35% vs the pre-optimization run (1t 2.38->3.30, 14t 24.7->33.4); scaling shape unchanged
# (the ~10x/14t taper is the machine memory-core-heterogeneity ceiling, not the formatter).
MEASURED = [
    (1, 3.304), (2, 6.313), (3, 9.172), (4, 12.072), (5, 15.107), (6, 18.134), (7, 19.836),
    (8, 21.800), (9, 23.908), (10, 25.753), (11, 27.746), (12, 29.431), (13, 31.256), (14, 33.428),
]
# Pre-optimization reference (Default parser, old kernel), same config -- plotted as a faint baseline.
BASELINE = [
    (1, 2.382), (2, 4.506), (3, 6.719), (4, 8.727), (5, 11.011), (6, 13.040), (7, 14.668),
    (8, 16.148), (9, 17.539), (10, 18.890), (11, 20.511), (12, 21.799), (13, 23.371), (14, 24.709),
]


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) >= 2 and p[0].isdigit():
                rows.append((int(p[0]), float(p[1])))
    return rows


def main():
    data_path = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "-" else None
    prefix = sys.argv[2] if len(sys.argv) > 2 else "scripts/benchmarking/formatter_scaling"
    rows = load(data_path) if data_path else MEASURED

    th = [r[0] for r in rows]
    gb = [r[1] for r in rows]
    base = gb[0]
    ideal_gb = [base * t for t in th]
    speedup = [g / base for g in gb]
    eff14 = speedup[-1] / th[-1]

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    title_sub = "SIMDCSV + FastUINT64 + bulk-movemask, 1-field parse (project f1 + fnattr), 128 KiB, in-memory\nApple M5 Max, 14-vCPU container"

    # ---- Plot 1: absolute throughput scalability ----
    fig1, ax1 = plt.subplots(figsize=(8.5, 5.5))
    ax1.plot(th, ideal_gb, "--", color="#999999", lw=1.8, label=f"ideal linear ({base:.2f} GB/s x threads)")
    if not data_path:
        bth = [r[0] for r in BASELINE]
        bgb = [r[1] for r in BASELINE]
        ax1.plot(bth, bgb, "-o", color="#bbbbbb", lw=1.8, ms=4, label="before opts (Default parser, old kernel)")
        ax1.annotate(
            f"{bgb[-1]:.1f} GB/s", xy=(bth[-1], bgb[-1]), xytext=(-6, -14),
            textcoords="offset points", ha="right", fontsize=9, color="#888888",
        )
    ax1.plot(th, gb, "-o", color="#1b7837", lw=2.4, ms=6, label="measured (+ FastUINT64 + bulk movemask)")
    ax1.annotate(
        f"{gb[-1]:.1f} GB/s",
        xy=(th[-1], gb[-1]),
        xytext=(-6, 8),
        textcoords="offset points",
        ha="right",
        fontsize=10,
        fontweight="bold",
        color="#1b7837",
    )
    ax1.set_xlabel("worker threads", fontsize=11)
    ax1.set_ylabel("formatter throughput (GB/s of input)", fontsize=11)
    ax1.set_title("Formatter throughput scaling to 14 threads\n" + title_sub, fontsize=11.5)
    ax1.set_xticks(th)
    ax1.set_xlim(0.5, 14.5)
    ax1.set_ylim(0, max(ideal_gb) * 1.02)
    ax1.grid(True, ls=":", alpha=0.45)
    ax1.legend(loc="upper left", fontsize=10)
    fig1.tight_layout()
    fig1.savefig(prefix + "_throughput.png", dpi=140)

    # ---- Plot 2: speedup vs ideal, linear scale (how close to linear) ----
    fig2, ax2 = plt.subplots(figsize=(8.5, 5.5))
    ax2.plot(th, th, "--", color="#999999", lw=1.8, label="ideal linear (y = threads)")
    ax2.plot(th, speedup, "-o", color="#1b7837", lw=2.4, ms=6, label="measured speedup")
    for t, s in zip(th, speedup):
        if t in (2, 4, 8, 14):
            ax2.annotate(f"{s:.1f}x", xy=(t, s), xytext=(4, -12), textcoords="offset points", fontsize=9, color="#1b7837")
    ax2.set_xlabel("worker threads", fontsize=11)
    ax2.set_ylabel("speedup vs 1 thread", fontsize=11)
    ax2.set_title(
        f"Scaling efficiency vs ideal linear  ({eff14*100:.0f}% at 14 threads)\n" + title_sub, fontsize=11.5
    )
    ax2.set_xticks(th)
    ax2.set_yticks(range(0, 15, 2))
    ax2.set_xlim(0.5, 14.5)
    ax2.set_ylim(0, 14.5)
    ax2.set_aspect("equal", adjustable="box")
    ax2.grid(True, ls=":", alpha=0.45)
    ax2.legend(loc="upper left", fontsize=10)
    fig2.tight_layout()
    fig2.savefig(prefix + "_speedup.png", dpi=140)

    print(f"wrote {prefix}_throughput.png and {prefix}_speedup.png")
    for t, g, s in zip(th, gb, speedup):
        print(f"  {t:>2}t: {g:6.2f} GB/s  {s:5.2f}x  ({s/t*100:4.0f}% eff)")


if __name__ == "__main__":
    main()
