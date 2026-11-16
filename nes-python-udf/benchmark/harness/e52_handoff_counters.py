# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# E52: is the extra process time of NES with 64 processes the cross-socket read of the hand-off?
# Boost off (E51). Shared processes, lcg. The buffer pool on node 0 (worker side, as before) or node 4 (process side).
# perf counters on the process CPUs for 5 s of the steady state; baseline A on the same CPUs for comparison.
import json, os, subprocess, time
EVENTS = "cycles:u,cycles:k,instructions:u,instructions:k,L1-dcache-load-misses,task-clock"
OUT = "results/E52.jsonl"
def perf(cpus, delay):
    time.sleep(delay)
    out = subprocess.run(["perf", "stat", "-a", "-C", cpus, "-x", ",", "-e", EVENTS, "--", "sleep", "5"], capture_output=True, text=True).stderr
    return {line.split(",")[2]: float(line.split(",")[0]) for line in out.strip().splitlines() if line.split(",")[0].replace(".", "").isdigit()}
def last():
    return json.loads(open(OUT).read().strip().splitlines()[-1])
def nes(layout, cpus, input_name, node, delay):
    command = ["python3", "harness/run_experiment.py", "--experiment", "E52", "--layouts", layout, "--udfs", "lcg", "--input", input_name, "--memory", "8589934592",
               "--worker-argument=--worker.python_udf.process_nursery=1M", "--environment", "NES_UDF_SHARED_PROCESSES=1", "--environment", f"UDF_BENCH_POOL_NODE={node}",
               "--note", f"shared, pool on node {node}, boost off, perf window"]
    load = os.getloadavg()[0]
    run = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    counters = perf(cpus, delay)
    print(run.communicate()[0].strip().splitlines()[-1], flush=True)
    record = last()
    q = record["mrows_per_s_by_quarter_second"]
    window = q[int(delay * 4) + 2:int((delay + 5) * 4) - 2]
    return counters, round(sum(window) / len(window), 2), command, load, record
def base(cpus, cpu_list, rows=2731):
    command = ["python3", "harness/udf_process_baseline.py", "--udf", "lcg", "--rows", str(rows), "--programs", "results/E49-programs", "--cpus", cpu_list,
               "--seconds", "8", "--environment", "PYPY_GC_NURSERY=1M", "--label", "E52 baseline A, boost off, perf window"]
    load = os.getloadavg()[0]
    run = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    counters = perf(cpus, 11)
    record = json.loads(run.communicate()[0])
    with open(OUT, "a") as out:
        out.write(json.dumps(record) + "\n")
    return counters, record["mrows_per_s"], command, load, record
cases = [
    ("nes 64, pool node 0", "64-127", lambda: nes("S w6 n4-7 smt", "64-127", "1920m", 0, 6)),
    ("nes 64, pool node 4", "64-127", lambda: nes("S w6 n4-7 smt", "64-127", "1920m", 4, 6)),
    ("baseline A 64", "64-127", lambda: base("64-127", ",".join(map(str, range(64, 128))))),
    ("nes 16, pool node 0", "64-79", lambda: nes("S w6 n4 smt", "64-79", "480m", 0, 6)),
    ("nes 16, pool node 4", "64-79", lambda: nes("S w6 n4 smt", "64-79", "480m", 4, 6)),
    ("baseline A 16", "64-79", lambda: base("64-79", ",".join(map(str, range(64, 80))))),
]
for name, cpus, call in cases:
    counters, mrows, command, load, record = call()
    rows = mrows * 1e6 * 5
    result = {"experiment": "E52", "perf": True, "case": name, "mrows_per_s": mrows, "window_seconds": 5, "cpus": cpus, "command": command, "load_before": load,
              "counters": counters, "per_row": {k: round(v / rows, 3) for k, v in counters.items() if k != "task-clock"},
              "ghz": round(counters.get("cycles:u", 0) / (counters["task-clock"] * 1e6) + counters.get("cycles:k", 0) / (counters["task-clock"] * 1e6), 3),
              "process_ns_per_row": record.get("process_ns_per_row"), "answer_ns_per_row": (record["answer_ns_per_batch"] / record["rows_per_batch"]) if record.get("answer_ns_per_batch") else None,
              "pools_bound": record.get("pools_bound")}
    with open(OUT, "a") as out:
        out.write(json.dumps(result) + "\n")
    p = result["per_row"]
    print(f"{name}: {mrows} M rows/s, {result['ghz']} GHz, per row cycles u/k {p.get('cycles:u')}/{p.get('cycles:k')}, instr u {p.get('instructions:u')}, L1 miss {p.get('L1-dcache-load-misses')}, process ns {result['process_ns_per_row']}, answer ns {result['answer_ns_per_row']}, load {load:.1f}", flush=True)
