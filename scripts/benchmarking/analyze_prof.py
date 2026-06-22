# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TMP DIAGNOSTIC (revert before merge). Analyze prof_threads.sh output:
#   lines = "elapsed_ms tid cpu_ticks comm"
# Computes per-thread CPU utilization over the EXECUTION window only -- defined
# as the set of sampling intervals in which at least one WorkerThread gained CPU
# ticks (so the single-threaded Nautilus-compile / idle startup is excluded).
# util = thread_ticks_delta / (interval_seconds * CLK_TCK), averaged over the
# window. Prints per-thread mean util and the window duration so we can see
# whether workers are saturated (~100%) or idle (supply-starved) at 2+2.

import sys
import os
from collections import defaultdict

CLK_TCK = float(os.environ.get("CLK_TCK", "100"))


def load(path):
    # tid -> list of (ms, ticks, comm)
    series = defaultdict(list)
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 4 or not parts[0].isdigit():
                continue
            ms, tid, ticks = int(parts[0]), parts[1], int(parts[2])
            comm = " ".join(parts[3:])
            series[tid].append((ms, ticks, comm))
    return series


def main(path):
    series = load(path)
    # Build the sorted set of sample timestamps shared across threads.
    times = sorted({ms for s in series.values() for (ms, _, _) in s})
    if len(times) < 3:
        print(f"{path}: too few samples")
        return
    # For each thread, a dict ms->ticks and the comm.
    bythread = {}
    for tid, s in series.items():
        d = {ms: ticks for (ms, ticks, _) in s}
        comm = s[-1][2]
        bythread[tid] = (d, comm)

    def is_worker(comm):
        return "Worker" in comm or "worker" in comm

    # Identify the tight execution window: the contiguous run of intervals whose TOTAL
    # tick-rate (all threads) is >= 50% of the peak total tick-rate. This excludes the
    # single-threaded Nautilus-compile/startup and the teardown, leaving the steady
    # full-parallelism formatting window.
    interval_rate = []  # (t0, t1, total_ticks_gained)
    for i in range(1, len(times)):
        t0, t1 = times[i - 1], times[i]
        if t1 - t0 <= 0 or t1 - t0 > 500:  # skip gaps from missed samples
            interval_rate.append((t0, t1, -1))
            continue
        total = 0
        for tid, (d, _comm) in bythread.items():
            if t0 in d and t1 in d:
                total += max(0, d[t1] - d[t0])
        rate = total / ((t1 - t0) / 1000.0)
        interval_rate.append((t0, t1, rate))
    peak = max((r for (_, _, r) in interval_rate), default=0)
    if peak <= 0:
        print(f"{path}: no activity")
        return
    thresh = 0.5 * peak
    # longest contiguous span above threshold
    best, cur = [], []
    for (t0, t1, r) in interval_rate:
        if r >= thresh:
            cur.append((t0, t1))
        else:
            if len(cur) > len(best):
                best = cur
            cur = []
    if len(cur) > len(best):
        best = cur
    exec_intervals = best
    if not exec_intervals:
        print(f"{path}: no execution window detected")
        return
    win_start, win_end = exec_intervals[0][0], exec_intervals[-1][1]
    win_s = (win_end - win_start) / 1000.0

    # Per-thread util over the execution window: sum ticks gained across exec intervals
    # divided by total exec-window seconds * CLK_TCK.
    rows = []
    for tid, (d, comm) in bythread.items():
        gained = 0
        covered_ms = 0
        for (t0, t1) in exec_intervals:
            if t0 in d and t1 in d:
                gained += max(0, d[t1] - d[t0])
                covered_ms += (t1 - t0)
        if covered_ms == 0:
            continue
        util = gained / (covered_ms / 1000.0 * CLK_TCK)
        rows.append((comm, tid, util, gained))
    rows.sort(key=lambda r: (r[0], -r[2]))

    print(f"=== {path}  window={win_s:.2f}s  ({len(exec_intervals)} intervals) ===")
    print(f"  {'comm':<18} {'tid':>8} {'util':>7}  cpu_s")
    worker_utils = []
    io_utils = []
    for comm, tid, util, gained in rows:
        cpu_s = gained / CLK_TCK
        if util < 0.02 and cpu_s < 0.05:
            continue  # skip idle bookkeeping threads
        print(f"  {comm:<18} {tid:>8} {util*100:6.1f}% {cpu_s:6.2f}")
        if is_worker(comm):
            worker_utils.append(util)
        if "IO" in comm or "io" in comm:
            io_utils.append(util)
    if worker_utils:
        print(f"  --> workers: n={len(worker_utils)} mean_util={sum(worker_utils)/len(worker_utils)*100:.1f}% "
              f"min={min(worker_utils)*100:.1f}% max={max(worker_utils)*100:.1f}%")
    if io_utils:
        print(f"  --> io:      n={len(io_utils)} mean_util={sum(io_utils)/len(io_utils)*100:.1f}%")
    print()


if __name__ == "__main__":
    for p in sys.argv[1:]:
        main(p)
