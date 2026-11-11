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

    python3 pypy_scaling.py --udf lcg --cpus 16,18,20 [--python /usr/bin/pypy3] [--seconds 5] [--environment NAME=VALUE]

`--udf inc+lcg+inc` runs a chain of calls, with one pass over the batch for each call.

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
SOURCES = {
    "inc": "def udf(x):\n    return x + 1\n",
    "lcg": "def udf(x):\n    h = x\n    for _ in range(100):\n        h = (h * 1103515245 + 12345) % 2147483648\n    return h\n",
}
# One function object per call of a chain, each from a compilation of its own, as the UDF operators of a chain have
udfs = []
for name in udf_name.split("+"):
    namespace = {}
    exec(compile(SOURCES[name], "<udf%d>" % len(udfs), "exec"), namespace)
    udfs.append(namespace["udf"])
first, later = udfs[0], udfs[1:]
batch = array.array("q", range(2731))
rows = 0
measured = None
while True:
    # One pass over the batch per call, as one UDF operator per call does; the query adds 1 between the calls
    r = [first(x) for x in batch]
    for udf in later:
        r = [udf(x + 1) for x in r]
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
    parser.add_argument("--udf", default="lcg", help="inc, lcg, or a chain of calls such as inc+lcg+inc, innermost first")
    parser.add_argument("--environment", action="append", default=[], metavar="NAME=VALUE", help="environment variable of the interpreters, e.g. PYPY_GC_NURSERY=1M")
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
            env=dict(os.environ, **dict(setting.split("=", 1) for setting in arguments.environment)),
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
                "calls": arguments.udf.count("+") + 1,
                "environment": arguments.environment,
                "mrows_per_s": round(sum(1e3 / value for value in values), 2),
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
