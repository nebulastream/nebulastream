# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs in place of the bootstrap of a UDF process: reads the program the worker sends, as the bootstrap does, and wraps its
functions with timers before it starts. When the process ends, it writes what it measured to $RUN_DIR/stats<index>.json:
rows and batches, the time in _process_batch (process), publishing answers (answer) and waiting for requests (wait), the first
and last batch, its rows by quarter second (series), and its CPU time. The first process also saves the generated program as $RUN_DIR/program.py.
"""

import os
import socket
import struct

sock = socket.socket(fileno=3)


def read(size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise SystemExit(1)
        data += chunk
    return bytes(data)


program = read(struct.unpack("<Q", read(8))[0]).decode()
if os.environ["UDF_INDEX"] == "0":
    with open(os.path.join(os.environ["RUN_DIR"], "program.py"), "w") as saved:
        saved.write(program)

INSTRUMENT = r'''
import time as _t, json as _json, os as _os, resource as _res
_pc = _t.perf_counter
_S = {"batches": 0, "rows": 0, "process": 0.0, "answer": 0.0, "wait": 0.0, "first": None, "last": 0.0, "series": {}}
# Rows by quarter second of the monotonic clock, which all processes share, so that the harness can leave out the warm-up
_series = _S["series"]
def _wrap(namespace, name, key, rows=False):
    original = namespace.get(name)
    if original is None:
        return
    def wrapped(*args):
        started = _pc()
        try:
            return original(*args)
        finally:
            ended = _pc()
            _S[key] += ended - started
            if rows:
                if _S["first"] is None:
                    _S["first"] = started
                _S["last"] = ended
                _S["batches"] += 1
                count = args[0] if isinstance(args[0], int) else args[0].num_rows
                _S["rows"] += count
                bucket = int(ended * 4)
                _series[bucket] = _series.get(bucket, 0) + count
    namespace[name] = wrapped
# By quarter second: wake-ups that a process sends to the worker, sleeps of the process, and its kernel and user time
_events_series = _S["events"] = {}
_last_bucket = [None]
def _event(index):
    bucket = int(_pc() * 4)
    entry = _events_series.get(bucket)
    if entry is None:
        usage = _res.getrusage(_res.RUSAGE_SELF)
        entry = _events_series[bucket] = [0, 0, usage.ru_stime, usage.ru_utime]
    entry[index] += 1
def _count_wakes(namespace):
    original = namespace.get("_wake_worker")
    if original is None:
        return
    control, word = namespace["_control"], namespace["_WORKER_SLEEPS"]
    def wrapped():
        if control[word]:
            _event(0)
        original()
    namespace["_wake_worker"] = wrapped
def _count_sleeps(namespace):
    original = namespace.get("_events")
    if original is None:
        return
    def wrapped(timeout):
        if timeout:
            _event(1)
        return original(timeout)
    namespace["_events"] = wrapped
# UDF_BENCH_POPULATE=1: a host process maps all pages of the buffer pool of the worker when it attaches its first operator, so
# that it has no page faults on the pool while it handles batches (MADV_POPULATE_WRITE is 23; UDF_BENCH_POPULATE=read uses MADV_POPULATE_READ, 22)
if _os.environ.get("UDF_BENCH_POPULATE") in ("1", "read") and "_pool" in globals():
    _original_pool = _pool
    def _pool(fd):
        known = len(_pools)
        view = _original_pool(fd)
        if len(_pools) > known:
            view.obj.madvise(22 if _os.environ["UDF_BENCH_POPULATE"] == "read" else 23)
        return view
_count_wakes(globals())
_count_sleeps(globals())
# UDF_BENCH_POOL_NODE=<node>: a host process binds the buffer pool of the worker to a NUMA node (mbind with MPOL_BIND) when it
# attaches its first operator. The pool is shared memory, so the policy holds for all processes and the worker. Automatic NUMA
# balancing does not scan memory with such a policy, so it no longer unmaps and moves the pages of the pool.
if _os.environ.get("UDF_BENCH_POOL_NODE") and "_pool" in globals():
    import ctypes as _ctypes
    _bound_pool = _pool
    def _pool(fd):
        known = len(_pools)
        view = _bound_pool(fd)
        if len(_pools) > known:
            libc = _ctypes.CDLL(None, use_errno=True)
            address = _ctypes.addressof(_ctypes.c_char.from_buffer(view.obj))
            mask = _ctypes.c_ulong(1 << int(_os.environ["UDF_BENCH_POOL_NODE"]))
            # mbind(address, length, MPOL_BIND, nodemask, maxnode, flags)
            if libc.syscall(237, _ctypes.c_void_p(address), _ctypes.c_ulong(len(view)), 2, _ctypes.byref(mask), 65, 0):
                raise OSError(_ctypes.get_errno(), "mbind of the buffer pool failed")
        return view
_wrap(globals(), "_process_batch", "process", True)
_wrap(globals(), "_answer", "answer")
_wrap(globals(), "_frame", "answer")
_wrap(globals(), "_await_request", "wait")
# A host process, which serves several UDF operators: each has a module with a _process_batch and an _answer of its own
_wrap(globals(), "_idle", "wait")
def _attached(operator):
    _wrap(operator.__dict__, "_process_batch", "process", True)
    _wrap(operator.__dict__, "_answer", "answer")
    _count_wakes(operator.__dict__)
_T0 = _pc()
try:
    (_serve if "_serve" in globals() else _main)()
finally:
    _S["wall"] = _pc() - _T0
    usage = _res.getrusage(_res.RUSAGE_SELF)
    _S.update(user=usage.ru_utime, sys=usage.ru_stime, vcsw=usage.ru_nvcsw, ivcsw=usage.ru_nivcsw)
    with open(_os.path.join(_os.environ["RUN_DIR"], "stats" + _os.environ["UDF_INDEX"] + ".json"), "w") as _out:
        _out.write(_json.dumps(_S))
'''
for ending in ("\n_serve()", '\nif not globals().get("_HOSTED"):\n    _main()', "\n_main()"):
    head, sep, tail = program.rpartition(ending)
    if sep:
        break
assert sep, "the program of the UDF process does not end in a call of _main() or _serve()"
exec(compile(head + INSTRUMENT + tail, "<nes-udf-process>", "exec"), {"sock": sock})
