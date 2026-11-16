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
`start()` also registers `dump()` to run at exit. With UDF_BENCH_PYPROF_MODE=vmprof, <file> is a vmprof profile of PyPy's built-in
profiler instead (UDF_BENCH_VMPROF_NATIVE=1 adds the native stacks), which the vmprof package reads.

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
_vmprof_file = None


def _name(code):
    return "%s (%s:%d)" % (code.co_name, os.path.basename(code.co_filename), code.co_firstlineno)


def _handler(signum, frame):
    stack = []
    while frame is not None:
        stack.append(frame.f_code)
        frame = frame.f_back
    _samples[tuple(stack)] += 1


def start():
    global _path, _vmprof_file
    _path = os.environ.get("UDF_BENCH_PYPROF")
    if not _path:
        return
    interval = float(os.environ.get("UDF_BENCH_PYPROF_INTERVAL", "0.001"))
    if os.environ.get("UDF_BENCH_PYPROF_MODE") == "vmprof":
        # The vmprof profiler of PyPy, through its built-in module: the vmprof package loads cpyext, which slows all compiled
        # loops (E46). The package reads the file afterwards in another process.
        import _vmprof

        _vmprof_file = open(_path, "w+b")
        native = os.environ.get("UDF_BENCH_VMPROF_NATIVE") == "1"
        _vmprof.enable(_vmprof_file.fileno(), interval, False, False, native, False)
        atexit.register(dump)
        return
    signal.signal(signal.SIGPROF, _handler)
    signal.setitimer(signal.ITIMER_PROF, interval, interval)
    atexit.register(dump)


def dump():
    global _dumped
    if not _path or _dumped:
        return
    _dumped = True
    if _vmprof_file is not None:
        import _vmprof

        _vmprof.disable()
        _vmprof_file.close()
        return
    signal.setitimer(signal.ITIMER_PROF, 0, 0)
    with open(_path, "w") as out:
        for stack, count in _samples.items():
            out.write(";".join(_name(code) for code in reversed(stack)) + " %d\n" % count)
