# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""How the per-row cost of a UDF changes with the number of interpreters running at once, without NebulaStream.

    python3 pypy_scaling.py --udf lcg --cpus 16,18,20 [--python /usr/bin/pypy3] [--seconds 5]

Starts one interpreter per CPU, pinned to it, each calling the UDF on batches of 2731 integers as a UDF process does
(`[udf(x) for x in batch]`), and prints one JSON line with the nanoseconds per row of every interpreter over the last
`--seconds`, after a warm-up of the same length. All interpreters start measuring at the same time.
"""

import argparse
import json
import os
import subprocess
import sys
import time

WORKER = r'''
import array, sys, time
udf_name, start, seconds = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
def inc(x):
    return x + 1
def lcg(x):
    h = x
    for _ in range(100):
        h = (h * 1103515245 + 12345) % 2147483648
    return h
udf = {"inc": inc, "lcg": lcg}[udf_name]
batch = array.array("q", range(2731))
rows = 0
measured = None
while True:
    r = [udf(x) for x in batch]
    now = time.time()
    if now < start:
        continue
    if measured is None:
        measured = now
        rows = 0
        continue
    rows += len(batch)
    if now >= start + seconds:
        print((now - measured) / rows * 1e9)
        break
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--udf", choices=["inc", "lcg"], default="lcg")
    parser.add_argument("--cpus", required=True, help="comma-separated CPUs, one interpreter each")
    parser.add_argument("--python", default="/usr/bin/pypy3")
    parser.add_argument("--seconds", type=float, default=5.0)
    parser.add_argument("--label", default="")
    arguments = parser.parse_args()
    cpus = [int(cpu) for cpu in arguments.cpus.split(",")]
    start = time.time() + arguments.seconds
    processes = [
        subprocess.Popen(
            ["taskset", "-c", str(cpu), arguments.python, "-c", WORKER, arguments.udf, str(start), str(arguments.seconds)],
            stdout=subprocess.PIPE,
            text=True,
        )
        for cpu in cpus
    ]
    values = [float(process.communicate()[0]) for process in processes]
    commit = subprocess.run(["git", "-C", os.path.dirname(os.path.abspath(__file__)), "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    print(
        json.dumps(
            {
                "label": arguments.label,
                "udf": arguments.udf,
                "cpus": cpus,
                "python": arguments.python,
                "seconds": arguments.seconds,
                "commit": commit,
                "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "mean_ns_per_row": round(sum(values) / len(values), 1),
                "min_ns_per_row": round(min(values), 1),
                "max_ns_per_row": round(max(values), 1),
                "ns_per_row": [round(value, 1) for value in values],
            }
        )
    )


if __name__ == "__main__":
    sys.exit(main())
