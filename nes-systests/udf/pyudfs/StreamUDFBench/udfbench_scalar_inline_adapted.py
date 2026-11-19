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
"""Adapted versions of the UDFBench scalar UDFs that the compilers (Codon for EXECUTION INLINE, Numba with nes_numba_ext) do not
accept as they are: CREATE FUNCTION ... ENTRYPOINT 'StreamUDFBench.udfbench_scalar_inline_adapted.<name>'.

Each function gives the same result as the UDFBench file in scalar/ for the inputs of StreamUDFBenchScalarUdfs.test, with
these changes:

- addnoise: imports random at module level, not inside the function (NES rejects an import inside an INLINE function).
- addnoise and jaccard: the None result comes first. Codon gives a function the type of its first return, so a float
  first makes a later None fail at run time.
- jaccard: puts the parsed elements into the sets as they are. UDFBench converts a nested list to a tuple with
  `tuple(x) if type(x) == list else x`, and a compiler decides `type(x) == list` at compile time. A JSON value of the json
  modules of Codon and nes_numba_ext is hashable, also as an array, so it needs no conversion.
- jsonparse: asks the parsed value for its kind with is_list() and is_dict() instead of isinstance, which a compiler also
  decides at compile time, reads a member with `in` and `[]`, and the first element of an array with `[0]` instead of a loop
  that returns in its first pass.

jsonparse needs the JSON value API of these json modules. Under CPython, json.loads returns a dict or a list, which have no
is_list(), so the OUT_OF_PROCESS and IN_PROCESS executions use the unchanged functions (udfbench_scalar.py).

The arguments are declared NOT NULL, so None does not reach these functions (as in UDFBench, which runs them in DuckDB).
"""

import json
import random


def addnoise(val):
    if not val:
        return None
    return float(val) + random.gauss(0, 2)


def jaccard(arg1, arg2):
    result = None
    try:
        rset = set(json.loads(arg1))
        sset = set(json.loads(arg2))
        result = float(len(rset & sset)) / len(rset | sset)
    except:  # noqa: E722 - as UDFBench: any error gives NULL
        pass
    return result


def _member(data, key):
    """The text of member `key` of a JSON object, or None if it has no such member or the member is null."""
    if key not in data:
        return None
    value = data[key]
    if value.is_null():
        return None
    return str(value)


def jsonparse(json_content, key1):
    try:
        data = json.loads(json_content)
        if data.is_list():
            if len(data) == 0:
                return None
            return _member(data[0], key1)
        if data.is_dict():
            return _member(data, key1)
        return None
    except:  # noqa: E722 - as UDFBench: any error gives NULL
        return None
