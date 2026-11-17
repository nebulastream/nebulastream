# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Entry points of the UDFBench scalar UDFs for EXECUTION INLINE (Codon): CREATE FUNCTION ... ENTRYPOINT
'StreamUDFBench.udfbench_scalar_inline.<name>'.

udfbench_scalar.py finds the files in scalar/ at runtime with importlib, which Codon cannot compile. This module imports the same
unchanged files statically. Their functions are methods of UDFBench's class Scalar and do not use `self`, so None stands in for it.

Codon enforces the `str` annotations of the arguments, so a NULL argument (None) cannot reach the UDFBench functions. Each of them
returns None for None under CPython (through `if arg:` or its `except`), so the entry points return None for it themselves.
"""

from StreamUDFBench.scalar.cleandate import cleandate as _cleandate
from StreamUDFBench.scalar.extractday import extractday as _extractday
from StreamUDFBench.scalar.extractmonth import extractmonth as _extractmonth
from StreamUDFBench.scalar.extractyear import extractyear as _extractyear
from StreamUDFBench.scalar.jsoncount import jsoncount as _jsoncount


def extractyear(arg):
    if arg is None:
        return None
    return _extractyear(None, arg)


def extractmonth(arg):
    if arg is None:
        return None
    return _extractmonth(None, arg)


def extractday(arg):
    if arg is None:
        return None
    return _extractday(None, arg)


def cleandate(pubdate):
    if pubdate is None:
        return None
    return _cleandate(None, pubdate)


def jsoncount(jval):
    if jval is None:
        return None
    return _jsoncount(None, jval)
