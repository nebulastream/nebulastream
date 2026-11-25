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

"""Prints one table row per run from results.jsonl files: correctness vs gap 0, pool peak, latency, holds."""
import json
import sys


def load(paths):
    rows = []
    for path in paths:
        with open(path) as f:
            rows += [dict(json.loads(line), _file=path) for line in f if line.strip()]
    return rows


def refresh(rows, paths):
    """Recomputes producer stats from the saved CSVs, so a metric fix applies to old runs."""
    import os

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import bench

    for r in rows:
        out_dir = os.path.join(os.path.dirname(r["_file"]), r["run"])
        spec_path = os.path.join(out_dir, "spec.json")
        if os.path.exists(spec_path):
            spec = json.load(open(spec_path))["scenario"]
            r.update(bench.producer_stats(out_dir, [s for s in spec["sources"] if s["transport"] != "file"]))
            if os.path.exists(os.path.join(out_dir, "arrivals.csv")):
                r.update(bench.latency([(0, 0)], out_dir, spec))


def main():
    rows = load(sys.argv[1:])
    refresh(rows, sys.argv[1:])
    base = {r["scenario"]: r for r in rows if r["gap_ms"] == 0}
    hdr = f"{'scenario':<18}{'gap':>6} {'ok':<5}{'rows':>7} {'pool%':>7} {'lat50':>7} {'exc50':>7} {'exc99':>7} {'excMax':>7} {'holds':>6} {'blocked s (src)':<24} {'prodLag/sendBlk s':<22} {'cpu':>6} {'rssMiB':>7}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        b = base.get(r["scenario"])
        if r.get("error"):
            ok = "ERR"
        elif b and b is not r and (b["rows"], b["checksum"]) != (r["rows"], r["checksum"]):
            ok = "DIFF"
        else:
            ok = "ok"
        pool = max(r.get("pool_peak_pct", {}).values(), default=0)
        blocked = ",".join(f"{k}:{v:.0f}" for k, v in sorted(r.get("blocked_s", {}).items()))
        blk = r.get("producer_blocked_s", {})
        lag = ",".join(f"{k}:{v:.1f}/{blk.get(k, 0):.1f}" for k, v in sorted(r.get("producer_max_lag_s", {}).items()) if v >= 0.05 or blk.get(k, 0) >= 1)
        f = lambda x: "-" if x is None else f"{x:.2f}"
        print(
            f"{r['scenario']:<18}{r['gap_ms']:>6} {ok:<5}{r['rows']:>7} {pool:>7.1f} {f(r.get('lat_p50_s')):>7} {f(r.get('excess_p50_s')):>7} "
            f"{f(r.get('excess_p99_s')):>7} {f(r.get('excess_max_s')):>7} {r.get('holds', 0):>6} {blocked:<24} {lag:<22} {r.get('cpu_s') or 0:>6.1f} {r.get('peak_rss_mib', 0):>7.0f}"
        )
        if r.get("error"):
            print(f"{'':<24}error: {r['error'][:140]}")


if __name__ == "__main__":
    main()
