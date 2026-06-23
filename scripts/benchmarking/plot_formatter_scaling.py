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
# (project f1 + fnattr), 128 KiB buffers, 5 GiB file (bench_5xUINT64_5g.csv), LEAN drain with RECYCLED
# output buffers (whole pool pre-faulted once -> no per-rep page-fault artifact), pre-loaded input buffers
# (no IO), median of 5 reps + 1 discarded warmup, drain-only timing.
# WITH the TracedIntegerInputParsers plugin: TracedUINT64 = fast_float/simdjson SWAR digit logic emitted
# as TRACED nautilus ops, so it fuses into the JIT'd scan loop (no proxy call -- the only way to inline
# the parse on ARM, where the nautilus InliningPass is disabled) + NEON vpaddq-bulk movemask index kernel.
# vs FastUINT64 (fast_float via proxy, NOT inlined on ARM), same clean methodology: 1t 3.60->4.60 (+28%),
# 14t 37.9->47.4 (+25%). The 10.3x/14t taper (~74% eff) is the machine memory-core-heterogeneity ceiling,
# not the formatter; leaner per-thread work hits it a hair sooner than FastUINT64's 10.5x (~75%).
MEASURED = [
    (1, 4.600), (2, 8.716), (3, 12.740), (4, 16.864), (5, 20.863), (6, 24.629), (7, 27.303),
    (8, 30.323), (9, 33.160), (10, 36.060), (11, 38.648), (12, 42.031), (13, 44.515), (14, 47.380),
]
# Prior best (FastUINT64 = fast_float via proxy), same clean lean+recycle methodology -- faint reference.
BASELINE = [
    (1, 3.595), (2, 6.972), (3, 10.439), (4, 13.676), (5, 16.770), (6, 19.531), (7, 21.839),
    (8, 24.352), (9, 26.661), (10, 28.956), (11, 31.060), (12, 33.613), (13, 36.176), (14, 37.867),
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

    title_sub = "SIMDCSV + TracedUINT64 (traced SWAR, inlined) + bulk-movemask, 1-field parse (project f1 + fnattr), 128 KiB, in-memory\nApple M5 Max, 14-vCPU container"

    # ---- Plot 1: absolute throughput scalability ----
    fig1, ax1 = plt.subplots(figsize=(8.5, 5.5))
    ax1.plot(th, ideal_gb, "--", color="#999999", lw=1.8, label=f"ideal linear ({base:.2f} GB/s x threads)")
    if not data_path:
        bth = [r[0] for r in BASELINE]
        bgb = [r[1] for r in BASELINE]
        ax1.plot(bth, bgb, "-o", color="#bbbbbb", lw=1.8, ms=4, label="FastUINT64 (fast_float via proxy, prior)")
        ax1.annotate(
            f"{bgb[-1]:.1f} GB/s", xy=(bth[-1], bgb[-1]), xytext=(-6, -14),
            textcoords="offset points", ha="right", fontsize=9, color="#888888",
        )
    ax1.plot(th, gb, "-o", color="#1b7837", lw=2.4, ms=6, label="TracedUINT64 (traced SWAR, inlined)")
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
