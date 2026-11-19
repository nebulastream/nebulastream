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
"""Compile times of the four UDFs of the Codon compile breakdown with Numba (nopython mode, eager signatures).

Prints one JSON line: wall and CPU milliseconds for `import numba` and for each compile, in the order of the Codon chart:
double, title, first_number (re), numpy score, numpy score again, title again, double again. "again" compiles the same
source as a new function, as a second registration would.
"""

import json
import sys
import time

timings = {}


def measure(name, action):
    wall = time.perf_counter()
    cpu = time.process_time()
    try:
        result = action()
        error = None
    except Exception as exception:  # noqa: BLE001 - an unsupported UDF is a result, not a failure
        result = None
        error = f"{type(exception).__name__}: {str(exception).splitlines()[0][:200]}"
    timings[name] = {
        "wall_ms": round((time.perf_counter() - wall) * 1000, 1),
        "cpu_ms": round((time.process_time() - cpu) * 1000, 1),
        "error": error,
    }
    return result


measure("import numba", lambda: __import__("numba"))
import numba  # noqa: E402
import numpy as np  # noqa: E402


def make_double():
    def double(x):
        return x * 2

    return double


def make_title():
    def title_words(s):
        words = s.split()
        return " ".join([w[:1].upper() + w[1:].lower() for w in words])

    return title_words


def make_first_number():
    import re

    def first_number(s):
        match = re.search(r"\d+", s)
        return int(match.group(0)) if match else -1

    return first_number


def make_score():
    def score(a, b):
        weights = np.array([0.5, -0.25])
        z = np.sum(weights * np.array([float(a), float(b)]))
        return float(1.0 / (1.0 + np.exp(-z)))

    return score


def compile_double():
    function = numba.njit("int64(int64)")(make_double())
    assert function(21) == 42
    return function


def compile_title():
    function = numba.njit("unicode_type(unicode_type)")(make_title())
    assert function("hello world 42") == "Hello World 42", function("hello world 42")
    return function


def compile_first_number():
    function = numba.njit("int64(unicode_type)")(make_first_number())
    return function


def compile_score():
    function = numba.njit("float64(int64, int64)")(make_score())
    assert abs(function(1, 2) - 0.5) < 1e-9
    return function


measure("double (1st UDF)", compile_double)
measure("title", compile_title)
measure("first_number (re)", compile_first_number)
measure("numpy score (1st)", compile_score)
measure("numpy score (2nd)", compile_score)
measure("title (again)", compile_title)
measure("double (again)", compile_double)

print(json.dumps({"python": sys.version.split()[0], "numba": numba.__version__, "timings": timings}))
