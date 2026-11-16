# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A sampling profiler of the Python stacks of a process, with the standard library only, for flamegraphs.

With UDF_BENCH_PYPROF=<file>, `start()` samples the stack of the main thread every UDF_BENCH_PYPROF_INTERVAL seconds of CPU time
(default 0.001, SIGPROF) and `dump()` writes the stacks in the collapsed format of flamegraph tools ("a;b;c count") to <file>.
`start()` also registers `dump()` to run at exit.

PyPy runs a signal handler only between bytecodes, also in compiled code, so a sample falls on the next such point after the
timer fires. Reading the frame in the handler makes PyPy build the frame objects of the stack; the overhead must be checked
against a run without the profiler.
"""

import atexit
import collections
import os
import signal

_samples = collections.Counter()
_path = None
_dumped = False


def _name(code):
    return "%s (%s:%d)" % (code.co_name, os.path.basename(code.co_filename), code.co_firstlineno)


def _handler(signum, frame):
    stack = []
    while frame is not None:
        stack.append(frame.f_code)
        frame = frame.f_back
    _samples[tuple(stack)] += 1


def start():
    global _path
    _path = os.environ.get("UDF_BENCH_PYPROF")
    if not _path:
        return
    interval = float(os.environ.get("UDF_BENCH_PYPROF_INTERVAL", "0.001"))
    signal.signal(signal.SIGPROF, _handler)
    signal.setitimer(signal.ITIMER_PROF, interval, interval)
    atexit.register(dump)


def dump():
    global _dumped
    if not _path or _dumped:
        return
    _dumped = True
    signal.setitimer(signal.ITIMER_PROF, 0, 0)
    with open(_path, "w") as out:
        for stack, count in _samples.items():
            out.write(";".join(_name(code) for code in reversed(stack)) + " %d\n" % count)
