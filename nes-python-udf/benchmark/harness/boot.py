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
and last batch, and its CPU time. The first process also saves the generated program as $RUN_DIR/program.py.
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
_S = {"batches": 0, "rows": 0, "process": 0.0, "answer": 0.0, "wait": 0.0, "first": None, "last": 0.0}
def _wrap(name, key, rows=False):
    original = globals().get(name)
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
                _S["rows"] += args[0] if isinstance(args[0], int) else args[0].num_rows
    globals()[name] = wrapped
_wrap("_process_batch", "process", True)
_wrap("_answer", "answer")
_wrap("_frame", "answer")
_wrap("_await_request", "wait")
_T0 = _pc()
try:
    _main()
finally:
    _S["wall"] = _pc() - _T0
    usage = _res.getrusage(_res.RUSAGE_SELF)
    _S.update(user=usage.ru_utime, sys=usage.ru_stime, vcsw=usage.ru_nvcsw, ivcsw=usage.ru_nivcsw)
    with open(_os.path.join(_os.environ["RUN_DIR"], "stats" + _os.environ["UDF_INDEX"] + ".json"), "w") as _out:
        _out.write(_json.dumps(_S))
'''
head, sep, tail = program.rpartition("\n_main()")
assert sep, "the program of the UDF process does not end in a call of _main()"
exec(compile(head + INSTRUMENT + tail, "<nes-udf-process>", "exec"), {"sock": sock})
