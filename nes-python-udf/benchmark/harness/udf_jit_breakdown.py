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
"""Worker-thread samples by category. Usage: udf_jit_breakdown.py PERF_DATA_JIT OBJECT."""
import collections, re, subprocess, sys
data, obj = sys.argv[1], sys.argv[2]
out = subprocess.run(["perf", "report", "--stdio", "--no-children", "-n", "-i", data, "--comms", "WorkerThread-0", "--sort", "dso,sym"],
                     capture_output=True, text=True).stdout
mapped = subprocess.run(["python3", sys.argv[0].replace("udf_jit_breakdown.py", "udf_jit_map.py"), data, obj, "0"], capture_output=True, text=True).stdout
cat = collections.Counter()
total = 0
for line in out.splitlines():
    m = re.match(r"\s+[\d.]+%\s+(\d+)\s+(\S+)\s+\[.\]\s+(.*)", line)
    if not m: continue
    n, dso, sym = int(m.group(1)), m.group(2), m.group(3)
    total += n
    if dso.startswith("jitted-") or dso == "[unknown]": continue  # counted from the mapping
    if re.search(r"malloc|free|realloc|NRT_MemInfo|NRT_Allocate|NRT_Free|seq_alloc|seq_realloc|allocatePythonUdfMemory|Arena::allocate", sym): c = "allocation"
    elif re.search(r"memmove|memcpy|memset|memchr", sym): c = "libc mem*"
    elif re.search(r"tolower|toupper|unicode|utf8proc|ExtendedCase|gettyperecord", sym): c = "unicode tables"
    elif "codon-json" in dso or "simdjson" in sym: c = "JSON plugin"
    elif dso == "[kernel.kallsyms]": c = "kernel"
    else: c = "NES and other"
    cat[c] += n
gen = collections.Counter()
for line in mapped.splitlines():
    m = re.match(r"\s+([\d.]+)%\s+(.*)", line)
    if m:
        name = m.group(2)
        k = "refcount (lock inc/dec)" if name.startswith("NRT_incref") or name.startswith("NRT_decref") else "generated code"
        gen[k] += float(m.group(1)) / 100
for k, v in gen.items(): cat[k] += round(v * total)
print(f"total {total}")
for k, v in cat.most_common(): print(f"  {k:26s} {v:7d}")
