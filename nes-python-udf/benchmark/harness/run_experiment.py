# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs out-of-process UDF benchmarks with pinned threads and processes and records every run.

    python3 run_experiment.py --experiment E20 --format native --udfs inc,lcg --layouts "P w7 p7" "Dw w3 p5" --reps 3

Each run executes one query of udf_csv.test or udf_native.test through systest on a single-node topology and appends one JSON
line to ../results/<experiment>.jsonl: the commit of the code under test, the full command line, the layout, the inputs and the
measurements. It refuses to run with uncommitted changes, so that the commit says what was measured; describe the experiment and
its outcome in ../EXPERIMENTS.md afterwards.

A layout says where things run on a machine with 16 hardware threads per NUMA node, even CPUs first thread of a core, odd CPUs
its sibling: `perf` are the performance CPUs (worker threads, one each), `misc` the CPUs of the source and other threads, `py`
the CPUs of the UDF processes (one each). --list prints all layouts.
"""

import argparse
import glob
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time

HARNESS = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HARNESS, "..", "..", ".."))
RESULTS = os.path.join(HARNESS, "..", "results")
CORES = [0, 2, 4, 6, 8, 10, 12, 14]

# query number in the test file and input rows, per (udf, source on a performance CPU)
QUERIES = {
    "csv": {
        "file": "udf_csv.test",
        "inputs": {"inc": ("udf/udf_bench_120m.csv", 120e6), "upper": ("udf/udf_bench_120m.csv", 120e6), "lcg": ("udf/udf_bench.csv", 30e6)},
        "numbers": {("inc", True): 1, ("upper", True): 2, ("lcg", True): 3, ("inc", False): 4, ("upper", False): 5, ("lcg", False): 6},
    },
    "native": {
        "file": "udf_native.test",
        "inputs": {
            "inc": ("udf/ids_480m.bin", 480e6),
            "lcg": ("udf/ids_60m.bin", 60e6),
            "scan": ("udf/ids_480m.bin", 480e6),
            "native_inc": ("udf/ids_480m.bin", 480e6),
        },
        "numbers": {("inc", True): 1, ("lcg", True): 2, ("inc", False): 3, ("lcg", False): 4, ("scan", False): 5, ("native_inc", False): 6},
    },
}


def node(number):
    """The first hardware thread of every core of a NUMA node."""
    return list(range(16 * number, 16 * number + 16, 2))


def layouts():
    out = []

    def add(name, perf, misc, py, src_perf=False):
        out.append({"name": name, "w": len(perf), "p": len(py), "perf": perf, "misc": misc, "py": py, "src_perf": src_perf})

    # A: file source on a core of its own, the UDF source and misc threads on another
    for w, p in [(5, 1), (4, 2), (3, 3), (2, 4), (1, 5)]:
        add(f"A w{w} p{p}", CORES[: w + 1], [CORES[w + 1]], CORES[w + 2 : w + 2 + p], src_perf=True)
    # B: both sources and misc share one hardware thread
    for w, p in [(6, 1), (5, 2), (4, 3), (3, 4), (2, 5), (1, 6)]:
        add(f"B w{w} p{p}", CORES[:w], [CORES[w]], CORES[w + 1 : w + 1 + p])
    # C: both sources and misc share one core, on both of its hardware threads
    for w, p in [(5, 2), (4, 3), (3, 4), (2, 5)]:
        add(f"C w{w} p{p}", CORES[:w], [CORES[w], CORES[w] + 1], CORES[w + 1 : w + 1 + p])
    # D: no core for sources and misc, they run on hyperthread siblings of worker cores (Dw) or of process cores (Dp)
    for w, p in [(6, 2), (5, 3), (4, 4), (3, 5)]:
        add(f"Dw w{w} p{p}", CORES[:w], [1, 3], CORES[w : w + p])
        add(f"Dp w{w} p{p}", CORES[:w], [13, 15], CORES[w : w + p])
    # E: worker threads on both hardware threads of their cores
    for wc, p in [(5, 2), (4, 3), (3, 4), (2, 5)]:
        add(f"E w{2 * wc} p{p}", [c + s for c in CORES[:wc] for s in (0, 1)], [CORES[wc], CORES[wc] + 1], CORES[wc + 1 : wc + 1 + p])
    # F: worker threads and processes on both hardware threads of their cores
    for wc, pc in [(5, 2), (4, 3), (3, 4), (2, 5)]:
        workers = [c + s for c in CORES[:wc] for s in (0, 1)]
        processes = [c + s for c in CORES[wc + 1 : wc + 1 + pc] for s in (0, 1)]
        add(f"F w{2 * wc} p{2 * pc}", workers, [CORES[wc], CORES[wc] + 1], processes)
    # G: few workers, many processes
    add("G w1 p7", [0], [1], CORES[1:])
    add("G w2 p6", [0, 1], [2, 3], CORES[2:])
    add("G w2 p12", [0, 1], [2, 3], [c + s for c in CORES[2:] for s in (0, 1)])
    # P: a worker thread and a process share each core, one on each hardware thread
    add("P w7 p7", CORES[:7], [14, 15], [c + 1 for c in CORES[:7]])
    add("P w8 p8", CORES, [14, 15], [c + 1 for c in CORES])
    # X: node 0 as before, extra processes on the cores of node 1
    add("X w2 p14", [0, 2], [1, 3], CORES[2:] + node(1))
    add("X w7 p8", CORES[:7], [14, 15], node(1))
    add("X w7 p15", CORES[:7], [14, 15], [c + 1 for c in CORES[:7]] + node(1))
    # R: the worker has node 0 to itself, the processes run on the cores of other nodes. Nodes 1-3 share the socket of node 0.
    for label, nodes in [("n1", [1]), ("n1-2", [1, 2]), ("n1-3", [1, 2, 3]), ("n4", [4]), ("n4-5", [4, 5]), ("n4-7", [4, 5, 6, 7]), ("n1-7", [1, 2, 3, 4, 5, 6, 7])]:
        processes = [cpu for number in nodes for cpu in node(number)]
        add(f"R w7 {label}", CORES[:7], [14, 15], processes)
        add(f"R w14 {label}", list(range(14)), [14, 15], processes)
    return out


def git(*arguments):
    return subprocess.run(["git", "-C", REPOSITORY, *arguments], capture_output=True, text=True, check=True).stdout.strip()


def sample_threads(process, threads):
    """Keeps the CPU time of every thread of the worker, by thread id and name."""
    while process.poll() is None:
        for task in glob.glob(f"/proc/{process.pid}/task/*"):
            try:
                with open(task + "/comm") as comm_file, open(task + "/stat") as stat_file:
                    comm = comm_file.read().strip()
                    fields = stat_file.read().rsplit(")", 1)[1].split()
                threads[(task.rsplit("/", 1)[1], comm)] = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
            except OSError:
                pass
        time.sleep(0.2)


def run(arguments, layout, udf, repetition, record_base):
    queries = QUERIES[arguments.format]
    input_file, rows = queries["inputs"][udf]
    label = f"{arguments.experiment}-{arguments.format}-{layout['name'].replace(' ', '-')}-{udf}-r{repetition}"
    run_dir = os.path.join(arguments.scratch, label)
    shutil.rmtree(run_dir, ignore_errors=True)
    os.makedirs(run_dir + "/work")
    environment = dict(os.environ, RUN_DIR=run_dir, BOOT=os.path.join(HARNESS, "boot.py"), UDF_BENCH_PYTHON=arguments.python)
    worker_arguments = [
        f"--worker.query_engine.number_of_worker_threads={layout['w']}",
        f"--worker.python_udf.processes={layout['p']}",
        f"--worker.python_udf.process_venv={os.path.join(HARNESS, 'venv')}",
        f"--worker.python_udf.process_protocol={arguments.protocol}",
        f"--worker.default_query_execution.operator_buffer_size={arguments.buffer}",
        f"--worker.total_memory_in_bytes={arguments.memory}",
        "--worker.query_engine.cpu_pinning=true",
        "--worker.query_engine.performance_cpus=" + ",".join(map(str, layout["perf"])),
        "--worker.query_engine.misc_cpus=" + ",".join(map(str, layout["misc"])),
        "--worker.python_udf.process_cpus=" + ",".join(map(str, layout["py"])),
        *arguments.worker_argument,
    ]
    command = [
        os.path.join(arguments.build, "nes-systests/systest/systest"),
        "-t",
        f"{os.path.join(HARNESS, queries['file'])}:{queries['numbers'][(udf, layout['src_perf'])]}",
        "--data",
        arguments.data,
        "--workingDir",
        run_dir + "/work",
        "--log-path",
        run_dir + "/log.txt",
        "-b",
        "--sequential",
        "-c",
        os.path.join(HARNESS, "single-node.yaml"),
        "--",
        *worker_arguments,
    ]
    with open(run_dir + "/out.txt", "w") as output:
        process = subprocess.Popen(command, env=environment, stdout=output, stderr=subprocess.STDOUT)
        threads = {}
        sampler = threading.Thread(target=sample_threads, args=(process, threads))
        sampler.start()
        return_code = process.wait()
        sampler.join()

    seconds = None
    for path in glob.glob(run_dir + "/work/**/BenchmarkResults.json", recursive=True):
        with open(path) as results:
            seconds = json.load(results)[0]["time"]
    statistics = []
    for path in glob.glob(run_dir + "/stats*.json"):
        try:
            with open(path) as file:
                statistics.append(json.load(file))
        except json.JSONDecodeError:
            pass  # a process that was killed before it wrote its statistics
    active = [s for s in statistics if s["first"] is not None]
    busy = [(s["process"] + s["answer"]) / max(s["last"] - s["first"], 1e-9) for s in active]
    batches = max(sum(s["batches"] for s in statistics), 1)
    processed = max(sum(s["rows"] for s in statistics), 1)
    cpu = {}
    for (_, comm), value in threads.items():
        cpu.setdefault("worker" if comm.startswith("WorkerThread") else comm, []).append(value)

    record = dict(
        record_base,
        layout=layout,
        udf=udf,
        repetition=repetition,
        input=input_file,
        rows=rows,
        command=command,
        return_code=return_code,
        seconds=seconds,
        mrows_per_s=round(rows / seconds / 1e6, 2) if seconds else None,
        rows_per_batch=round(processed / batches) if statistics else None,
        process_ns_per_row=round(sum(s["process"] for s in statistics) / processed * 1e9, 1) if statistics else None,
        answer_ns_per_batch=round(sum(s["answer"] for s in statistics) / batches * 1e9) if statistics else None,
        process_wait_fraction=round(sum(s["wait"] for s in active) / max(sum(s["last"] - s["first"] for s in active), 1e-9), 2) if active else None,
        process_busy=round(sum(busy) / len(busy), 2) if busy else None,
        process_busy_min=round(min(busy), 2) if busy else None,
        processes_reporting=len(statistics),
        worker_busy=round(sum(cpu.get("worker", [])) / (layout["w"] * seconds), 2) if seconds else None,
        source_threads_cpu=sorted(round(value / seconds, 2) for name, values in cpu.items() if name.startswith("DataSrc") for value in values) if seconds else None,
        misc_threads_cpu={name: round(sum(values) / seconds, 2) for name, values in cpu.items() if name in ("task-delayer", "net-sender", "net-receiver")} if seconds else None,
    )
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, arguments.experiment + ".jsonl"), "a") as results:
        results.write(json.dumps(record) + "\n")
    print(
        "%-12s %-10s r%d  %8s M rows/s  processes busy %s (min %s)  %s ns/row  workers busy %s  sources %s  rc %d"
        % (layout["name"], udf, repetition, record["mrows_per_s"], record["process_busy"], record["process_busy_min"], record["process_ns_per_row"], record["worker_busy"], record["source_threads_cpu"], return_code),
        flush=True,
    )
    if not arguments.keep:
        shutil.rmtree(run_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiment", help="id of the experiment in EXPERIMENTS.md, e.g. E20; names the results file")
    parser.add_argument("--list", action="store_true", help="print the layouts and exit")
    parser.add_argument("--format", choices=sorted(QUERIES), default="native", help="input format (default: native)")
    parser.add_argument("--udfs", default="inc,lcg", help="comma-separated UDFs: inc, upper (csv only), lcg, scan and native_inc (native only)")
    parser.add_argument("--layouts", nargs="+", default=[], metavar="NAME", help="layout names, see --list")
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--buffer", type=int, default=65536, help="operator_buffer_size in bytes (default: 65536)")
    parser.add_argument("--memory", type=int, default=4294967296, help="total_memory_in_bytes (default: 4 GiB)")
    parser.add_argument("--protocol", default="SHARED_MEMORY_ZERO_COPY")
    parser.add_argument("--python", default="/usr/bin/pypy3", help="interpreter of the UDF processes (default: /usr/bin/pypy3)")
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    parser.add_argument("--data", default="/dev/shm/nes-udf-perf-eval", help="directory with the inputs, on a RAM disk")
    parser.add_argument("--scratch", default="/tmp/nes-udf-experiments", help="directory for the files of the runs")
    parser.add_argument("--worker-argument", action="append", default=[], metavar="ARG", help="further worker configuration, e.g. --worker.x=y")
    parser.add_argument("--note", default="", help="free text stored with every record")
    parser.add_argument("--keep", action="store_true", help="keep the files of the runs, e.g. the generated program and the logs")
    arguments = parser.parse_args()

    by_name = {layout["name"]: layout for layout in layouts()}
    if arguments.list:
        for layout in by_name.values():
            print("%-12s workers %-40s misc %-10s processes %s%s" % (layout["name"], layout["perf"], layout["misc"], layout["py"], "  (file source on a performance CPU)" if layout["src_perf"] else ""))
        return
    if not arguments.experiment or not arguments.layouts:
        parser.error("--experiment and --layouts are required")
    unknown = [name for name in arguments.layouts if name not in by_name]
    if unknown:
        parser.error(f"unknown layouts: {unknown}")
    if git("status", "--porcelain", "--untracked-files=no"):
        sys.exit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")

    binary = os.path.join(arguments.build, "nes-systests/systest/systest")
    record_base = {
        "experiment": arguments.experiment,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "commit": git("rev-parse", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "binary": binary,
        "binary_modified": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(os.path.getmtime(binary))),
        "commit_time": git("log", "-1", "--format=%cI"),
        "format": arguments.format,
        "protocol": arguments.protocol,
        "buffer": arguments.buffer,
        "memory": arguments.memory,
        "python": subprocess.run([arguments.python, "--version"], capture_output=True, text=True, check=False).stdout.strip().replace("\n", " "),
        "kernel": platform.release(),
        "note": arguments.note,
    }
    for name in arguments.layouts:
        for udf in arguments.udfs.split(","):
            for repetition in range(arguments.reps):
                run(arguments, by_name[name], udf, repetition, record_base)


if __name__ == "__main__":
    main()
