# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The throughput of the generated programs of UDF processes without NebulaStream.

    python3 udf_process_baseline.py --udf inc+lcg --programs ../results/E28-programs --cpus 64,65 --rows 1638
        [--python /usr/bin/pypy3] [--seconds 5] [--environment NAME=VALUE]

Starts one interpreter per CPU, pinned to it. Each interpreter loads the generated program of every call of the chain as
a module of its own, as a shared UDF process does, with rings of the size the worker uses and one buffer in a common pool.
A driver in the same interpreter does the work of the worker for one batch after the other: it writes the request into the
ring of the first call, lets the program handle it (`_step()`: read the request, decode the column, call the function,
write the result column and the answer), takes the answer, copies the result column into the buffer of the next call, and
so on. There are no sinks, no sources, no wake-ups and no second process. The query adds 1 between the calls; the driver
does not.

`--programs` is a directory with `<udf>.py`, the generated program of a UDF process for that function, as
`run_experiment.py --keep` saves it (`program.py` of a run with separate processes).

Prints one JSON line with the nanoseconds per row of the query of every interpreter over the last `--seconds`, after a
warm-up of the same length. All interpreters start measuring at the same time.
"""

import argparse
import json
import os
import subprocess
import sys
import time

WORKER = r'''
import os as _os_pyprof, sys as _sys_pyprof
if _os_pyprof.environ.get("UDF_BENCH_PYPROF"):
    _sys_pyprof.path.insert(0, _os_pyprof.environ["UDF_BENCH_PYPROF_PATH"])
    import pyprof
    pyprof.start()
import mmap, os, struct, sys, time, types
directory, chain, rows, start, seconds = sys.argv[1], sys.argv[2].split("+"), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])
RING = 4 * 1024 * 1024   # RING_SIZE of PythonProcessShmSource.cpp
BUFFER = 65536           # one buffer of the worker per call
NONE = 0xFFFFFFFF
WRAP = 0xFFFFFFFFFFFFFFFF
INPUT = 64
VALIDITY = INPUT + rows * 8
OUTPUT = VALIDITY + ((rows + 7) // 8 + 7) // 8 * 8
assert OUTPUT + rows * 8 <= BUFFER, "the rows do not fit a buffer"


class Socket:
    # The program reports that it is ready and wakes a worker that sleeps; this worker never sleeps
    def sendall(self, data):
        pass
    send = sendall
    def setblocking(self, flag):
        pass


pool_fd = os.memfd_create("pool")
os.ftruncate(pool_fd, BUFFER * len(chain))
pool = memoryview(mmap.mmap(pool_fd, 0))
operators = []
for index, name in enumerate(chain):
    with open(os.path.join(directory, name + ".py")) as file:
        program = file.read()
    shared = os.memfd_create("rings")
    os.ftruncate(shared, 4096 + 2 * RING)
    operator = types.ModuleType("nes_udf_operator")
    operator.sock = Socket()
    operator._HOSTED = True
    operator._SHARED_FD = shared
    operator._POOL = pool
    exec(compile(program, "<nes-udf-operator-%d>" % index, "exec"), operator.__dict__)
    main = BUFFER * index
    # Where the columns are: (buffer, offset) of the validity and the values of the parameter and of the result
    struct.pack_into("<" + "II" * 6, pool, main, NONE, 0, 0, INPUT, NONE, 0, 0, VALIDITY, 0, OUTPUT, NONE, 0)
    operators.append((operator, main))
pool[INPUT:INPUT + rows * 8].cast("q")[:] = memoryview(bytes(8 * rows)).cast("q")
ids = pool[INPUT:INPUT + rows * 8].cast("q")
for row in range(rows):
    ids[row] = row
tails = [0] * len(operators)
size = rows * 8


def batch():
    for index, (operator, main) in enumerate(operators):
        tail = tails[index]
        position = tail % RING
        if RING - position < 48:
            struct.pack_into("<Q", operator._requests, position, WRAP)
            tail += RING - position
            operator._control[operator._REQUEST_TAIL] = tail
            operator._step()
            position = 0
        # [record size][rows][children][area][area size][main buffer]
        struct.pack_into("<QQQQQQ", operator._requests, position, 48, rows, 0, 0, 0, main)
        tail += 48
        tails[index] = tail
        operator._control[operator._REQUEST_TAIL] = tail
        operator._step()
        answered = operator._control[operator._ANSWER_TAIL]
        operator._control[operator._ANSWER_HEAD] = answered
        if index + 1 < len(operators):
            source = main + OUTPUT
            target = main + BUFFER + INPUT
            pool[target:target + size] = pool[source:source + size]


def check():
    # The first batch gives what the functions give without the programs
    functions = [operator._udf_0 for operator, _ in operators]
    batch()
    operator, main = operators[-1]
    position = (operator._control[operator._ANSWER_TAIL] - 64) % RING
    record, status, answered, entries = struct.unpack_from("<QQQQ", operator._answers, position)
    assert (record, status, answered, entries) == (64, 1, rows, 1), (record, status, answered, entries)
    results = pool[main + OUTPUT:main + OUTPUT + size].cast("q")
    for row in (0, 1, rows - 1):
        value = row
        for function in functions:
            value = function(value)
        assert results[row] == value, (row, results[row], value)


check()
done = 0
measured = None
while True:
    batch()
    now = time.time()
    if now < start:
        continue
    if measured is None:
        measured = now
        done = 0
        continue
    done += rows
    if now >= start + seconds:
        print((now - measured) / done * 1e9)
        break
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--udf", required=True, help="a UDF or a chain of calls such as inc+lcg+inc, innermost first")
    parser.add_argument("--programs", required=True, help="directory with the generated program <udf>.py of every UDF")
    parser.add_argument("--cpus", required=True, help="comma-separated CPUs, one interpreter each")
    parser.add_argument("--rows", type=int, required=True, help="rows per batch, e.g. rows_per_batch of the run to compare with")
    parser.add_argument("--python", default="/usr/bin/pypy3")
    parser.add_argument("--seconds", type=float, default=5.0)
    parser.add_argument("--environment", action="append", default=[], metavar="NAME=VALUE", help="environment variable of the interpreters, e.g. PYPY_GC_NURSERY=1M")
    parser.add_argument("--label", default="")
    arguments = parser.parse_args()
    cpus = [int(cpu) for cpu in arguments.cpus.split(",")]
    start = time.time() + arguments.seconds + 2
    command = [arguments.python, "-c", WORKER, os.path.abspath(arguments.programs), arguments.udf, str(arguments.rows), str(start), str(arguments.seconds)]
    environment = dict(os.environ, **dict(setting.split("=", 1) for setting in arguments.environment))
    # Only the first interpreter profiles (UDF_BENCH_PYPROF), like the first process of NebulaStream in boot.py
    quiet = {name: value for name, value in environment.items() if name != "UDF_BENCH_PYPROF"}
    processes = [
        subprocess.Popen(["taskset", "-c", str(cpu)] + command, stdout=subprocess.PIPE, text=True, env=environment if index == 0 else quiet)
        for index, cpu in enumerate(cpus)
    ]
    outputs = [process.communicate()[0] for process in processes]
    if any(process.returncode for process in processes):
        sys.exit("an interpreter failed")
    values = [float(output) for output in outputs]
    commit = subprocess.run(["git", "-C", os.path.dirname(os.path.abspath(__file__)), "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    print(
        json.dumps(
            {
                "label": arguments.label,
                "baseline": "udf_process_baseline.py",
                "udf": arguments.udf,
                "calls": arguments.udf.count("+") + 1,
                "rows_per_batch": arguments.rows,
                "programs": arguments.programs,
                "environment": arguments.environment,
                "cpus": cpus,
                "python": arguments.python,
                "seconds": arguments.seconds,
                "commit": commit,
                "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "mrows_per_s": round(sum(1e3 / value for value in values), 2),
                "mean_ns_per_row": round(sum(values) / len(values), 1),
                "min_ns_per_row": round(min(values), 1),
                "max_ns_per_row": round(max(values), 1),
                "ns_per_row": [round(value, 1) for value in values],
            }
        )
    )


if __name__ == "__main__":
    sys.exit(main())
