# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs ../numa_handoff.c over the placements of the pools, queue depths, read strides and answer modes.

    python3 numa_handoff_matrix.py --experiment E37 [--reps 3]

Compiles the program, runs every case `--reps` times, appends every run to ../results/<experiment>.jsonl with the commit and the
command, and prints the median nanoseconds per buffer of every case. The writer is on CPU 2 (NUMA node 0). The reader is on CPU 64
(node 4, the other socket) or on CPU 16 (node 1, the same socket). W is the node of the writer, R the node of the reader.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
STEPS = ["wall", "fill", "collect", "read", "write", "move"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--reps", type=int, default=3)
    arguments = parser.parse_args()
    if subprocess.run(["git", "-C", HERE, "status", "--porcelain", "--untracked-files=no", "--", ".."], capture_output=True, text=True).stdout.strip():
        sys.exit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")
    commit = subprocess.run(["git", "-C", HERE, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    binary = os.path.join(tempfile.mkdtemp(), "numa_handoff")
    subprocess.run(["gcc", "-O2", "-o", binary, os.path.join(HERE, "..", "numa_handoff.c")], check=True)
    results = os.path.join(HERE, "..", "results", arguments.experiment + ".jsonl")

    def run(label, reps=None, **settings):
        command = [binary] + [f"{name}={value}" for name, value in settings.items()]
        records = []
        for repetition in range(reps or arguments.reps):
            record = json.loads(subprocess.run(command, capture_output=True, text=True, check=True).stdout)
            record.update(experiment=arguments.experiment, commit=commit, repetition=repetition, label=label, command=command[1:], started=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
            with open(results, "a") as out:
                out.write(json.dumps(record) + "\n")
            records.append(record)
        medians = {step: round(statistics.median(record[step + "_ns_per_buffer"] for record in records)) for step in STEPS}
        print("%-74s %s  migrated %d" % (label, "  ".join("%s %6d" % item for item in medians.items()), records[-1]["pages_migrated"]), flush=True)

    for reader_cpu, reader_node, pair in [(64, 4, "other socket"), (16, 1, "same socket")]:
        common = dict(writer_cpu=2, reader_cpu=reader_cpu)
        places = [("W", 0), ("R", reader_node)]
        print(f"== {pair}: one direction, the reader reads all of the buffer")
        for buffer, count in [(16384, 200000), (65536, 60000)]:
            for depth in (1, 16, 1024, 4096):
                if buffer * depth > 1 << 28:
                    continue
                for name, node in places:
                    run(f"{pair} | {buffer >> 10} KB | depth {depth} | request on {name}", **common, request_node=node, buffer=buffer, depth=depth, buffers=count)
        print(f"== {pair}: read stride, 64 KB buffers")
        for depth in (16, 1024):
            for stride in (8, 64, 4096):
                for name, node in places:
                    run(f"{pair} | 64 KB | depth {depth} | stride {stride} | request on {name}", **common, request_node=node, buffer=65536, depth=depth, stride=stride, buffers=60000)
        print(f"== {pair}: answers, 16 KB buffers, the reader reads one field of every row of 16 bytes")
        for depth in (16, 1024):
            for name, node in places:
                run(f"{pair} | depth {depth} | in place | buffer on {name}", **common, request_node=node, buffer=16384, depth=depth, stride=16, answer="inplace")
            for (request_name, request_node) in places:
                for (result_name, result_node) in places:
                    run(f"{pair} | depth {depth} | separate | request on {request_name}, result on {result_name}", **common, request_node=request_node, result_node=result_node, buffer=16384, depth=depth, stride=16, answer="separate")
    print("== other socket: the writer moves the pages")
    common = dict(writer_cpu=2, reader_cpu=64, request_node=0, result_node=4)
    run("move to the reader | 16 KB (4 pages per call)", reps=1, **common, move="reader", buffer=16384, pool=1 << 28, buffers=30000)
    run("move to the reader and back | 16 KB", reps=1, **common, move="both", buffer=16384, pool=1 << 28, buffers=30000)
    run("move to the reader | 256 KB (64 pages per call), one pass over the pool", reps=1, **common, move="reader", buffer=262144, pool=1 << 30, buffers=4096)
    run("no move | 256 KB", reps=1, **common, buffer=262144, pool=1 << 30, buffers=4096)


if __name__ == "__main__":
    main()
