#!/usr/bin/env bash
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

# Compiles UDFBench UDFs with Numba into object files without the Python wrapper (build.py, Numba's Python environment), links
# each with Numba's C runtime (NRT, typed list and dict, Unicode tables) and runtime.c into a program without libpython
# (driver.cpp), runs it over the tiny data and compares every row with CPython. Usage: run.sh PYTHON OUTDIR UDF...
set -euo pipefail
python="$1"; out="$2"; shift 2
here="$(cd "$(dirname "$0")" && pwd)"
numba="$("$python" -c 'import numba, os; print(os.path.dirname(numba.__file__))')"
include="$("$python" -c 'import sysconfig; print(sysconfig.get_paths()["include"])')"
mkdir -p "$out"
clang-22 -O2 -fPIC -c "$here/runtime.c" -I"$numba" -o "$out/runtime.o"
for file in dictobject listobject utils; do
    clang-22 -O2 -fPIC -c "$numba/cext/$file.c" -I"$numba" -I"$numba/cext" -I"$include" -o "$out/$file.o"
done
clang++-22 -O2 -fPIC -c "$numba/core/runtime/nrt.cpp" -I"$numba" -I"$numba/core/runtime" -o "$out/nrt.o"
clang++-22 -O2 -std=c++20 -c "$here/driver.cpp" -o "$out/driver.o"
for udf in "$@"; do
    PYTHONPATH="$here/.." "$python" "$here/build.py" "$udf" "$out/$udf"
    clang++-22 -o "$out/$udf/driver" "$out/driver.o" "$out/$udf/entry.o" "$out"/{runtime,dictobject,listobject,utils,nrt}.o
    echo "$udf links libpython: $(ldd "$out/$udf/driver" | grep -c python || true)"
    "$out/$udf/driver" "$out/$udf" 3
    PYTHONPATH="$here/.." "$python" "$here/build.py" --compare "$out/$udf"
done
