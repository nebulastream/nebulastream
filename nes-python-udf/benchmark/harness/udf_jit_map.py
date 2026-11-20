# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Maps worker-thread samples in the JIT code region onto an object compiled from the dumped IR.

Nautilus' perf support records only the exported functions of a pipeline. The local functions of a UDF module (Numba's
nes_numba_entry, NRT_incref and NRT_decref, Codon's helpers) appear as [unknown]. To map them, dump the pipeline IR in the same
configuration (worker options dump_compilation_result=FILE and nautilus_perf=true) and compile the dumped
after_llvm_generation.ll with `llc -O3 -code-model=large -mcpu=native -filetype=obj`: the layout is then the same as in the JIT.

Usage: udf_jit_map.py PERF_DATA_JIT OBJECT [TOP]. The base is the start of the jitted mapping of `execute`.
Prints the share of worker-thread samples per function and the hottest instructions with their disassembly."""
import collections
import re
import subprocess
import sys

data, obj = sys.argv[1], sys.argv[2]
top = int(sys.argv[3]) if len(sys.argv) > 3 else 40

mmaps = subprocess.run(["perf", "script", "-i", data, "--show-mmap-events"], capture_output=True, text=True).stdout
execute_size = next(int(l.split()[1], 16) for l in subprocess.run(["nm", "-S", obj], capture_output=True, text=True).stdout.splitlines()
                    if l.endswith(" execute"))
base = None
for line in mmaps.splitlines():
    m = re.search(r"PERF_RECORD_MMAP2 .*\[(0x[0-9a-f]+)\((0x[0-9a-f]+)\)", line)
    if m and "jitted-" in line and int(m.group(2), 16) == execute_size:
        base = int(m.group(1), 16)
assert base is not None, "no jitted mapping has the size of execute: the IR does not match the profile"

samples = subprocess.run(["perf", "script", "-i", data, "--comms", "WorkerThread-0", "-F", "ip"], capture_output=True, text=True).stdout
ips = [int(x, 16) for x in samples.split()]
total = len(ips)

symbols = []
for line in subprocess.run(["nm", "-S", "-n", obj], capture_output=True, text=True).stdout.splitlines():
    parts = line.split()
    if len(parts) == 4 and parts[2] in "tT":
        symbols.append((int(parts[0], 16), int(parts[1], 16), parts[3]))
end = max(s + n for s, n, _ in symbols)

per_function = collections.Counter()
per_offset = collections.Counter()
for ip in ips:
    offset = ip - base
    if 0 <= offset < end:
        per_offset[offset] += 1
        name = next((n for s, size, n in symbols if s <= offset < s + size), "?")
        per_function[name] += 1

demangle = lambda n: subprocess.run(["c++filt"], input=n, capture_output=True, text=True).stdout.strip()[:110]
print(f"worker samples {total}, in generated code {sum(per_function.values())} ({100 * sum(per_function.values()) / total:.1f}%)")
for name, count in per_function.most_common(12):
    print(f"  {100 * count / total:5.1f}%  {demangle(name)}")

disassembly = {}
for line in subprocess.run(["objdump", "-d", "--no-show-raw-insn", obj], capture_output=True, text=True).stdout.splitlines():
    m = re.match(r"\s+([0-9a-f]+):\s+(.*)", line)
    if m:
        disassembly[int(m.group(1), 16)] = m.group(2)
print("hottest instructions (offset, % of worker samples):")
for offset, count in per_offset.most_common(top):
    name = next((n for s, size, n in symbols if s <= offset < s + size), "?")
    print(f"  {offset:6x} {100 * count / total:5.2f}%  {disassembly.get(offset, '?')[:60]:60s} {demangle(name)[:50]}")
