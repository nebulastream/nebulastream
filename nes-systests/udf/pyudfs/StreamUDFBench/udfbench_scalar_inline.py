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

Codon enforces the annotations of the arguments (`str`, `int`, `float`), so a NULL argument (None) cannot reach the UDFBench
functions. Each entry point returns for None what the UDFBench function returns for None under CPython: None, or "[]" (jsort,
jsortvalues, removeshortterms) and 0.0 (log_10), which their `except` gives.

Codon cannot compile or run some of the UDFs, see the INLINE section of StreamUDFBenchScalarUdfs.test, and NES rejects addnoise
for INLINE, because it imports random inside the function. Their entry points are here all the same.
"""

from StreamUDFBench.scalar.addnoise import addnoise as _addnoise
from StreamUDFBench.scalar.clean import clean as _clean
from StreamUDFBench.scalar.cleandate import cleandate as _cleandate
from StreamUDFBench.scalar.converttoeuro import converttoeuro as _converttoeuro
from StreamUDFBench.scalar.extractclass import extractclass as _extractclass
from StreamUDFBench.scalar.extractcode import extractcode as _extractcode
from StreamUDFBench.scalar.extractday import extractday as _extractday
from StreamUDFBench.scalar.extractfunder import extractfunder as _extractfunder
from StreamUDFBench.scalar.extractid import extractid as _extractid
from StreamUDFBench.scalar.extractmonth import extractmonth as _extractmonth
from StreamUDFBench.scalar.extractprojectid import extractprojectid as _extractprojectid
from StreamUDFBench.scalar.extractyear import extractyear as _extractyear
from StreamUDFBench.scalar.filterstopwords import filterstopwords as _filterstopwords
from StreamUDFBench.scalar.frequentterms import frequentterms as _frequentterms
from StreamUDFBench.scalar.jaccard import jaccard as _jaccard
from StreamUDFBench.scalar.jpack import jpack as _jpack
from StreamUDFBench.scalar.jsoncount import jsoncount as _jsoncount
from StreamUDFBench.scalar.jsonparse import jsonparse as _jsonparse
from StreamUDFBench.scalar.jsort import jsort as _jsort
from StreamUDFBench.scalar.jsortvalues import jsortvalues as _jsortvalues
from StreamUDFBench.scalar.keywords import keywords as _keywords
from StreamUDFBench.scalar.log_10 import log_10 as _log_10
from StreamUDFBench.scalar.lowerize import lowerize as _lowerize
from StreamUDFBench.scalar.removeshortterms import removeshortterms as _removeshortterms
from StreamUDFBench.scalar.stem import stem as _stem


def addnoise(val):
    # UDFBench's addnoise returns None for 0 in a function annotated -> float, which Codon cannot unpack.
    if val is None or val == 0:
        return None
    return _addnoise(None, val)


def clean(val):
    if val is None:
        return None
    return _clean(None, val)


def cleandate(pubdate):
    if pubdate is None:
        return None
    return _cleandate(None, pubdate)


def converttoeuro(x, y):
    if x is None or y is None:
        return None
    return _converttoeuro(None, x, y)


def extractclass(project):
    if project is None:
        return None
    return _extractclass(None, project)


def extractcode(project):
    if project is None:
        return None
    return _extractcode(None, project)


def extractday(arg):
    if arg is None:
        return None
    return _extractday(None, arg)


def extractfunder(project):
    if project is None:
        return None
    return _extractfunder(None, project)


def extractid(project):
    if project is None:
        return None
    return _extractid(None, project)


def extractmonth(arg):
    if arg is None:
        return None
    return _extractmonth(None, arg)


def extractprojectid(input):
    if input is None:
        return None
    return _extractprojectid(None, input)


def extractyear(arg):
    if arg is None:
        return None
    return _extractyear(None, arg)


def filterstopwords(words):
    if words is None:
        return None
    return _filterstopwords(None, words)


def frequentterms(input, N):
    if input is None or N is None:
        return None
    return _frequentterms(None, input, N)


def jaccard(arg1, arg2):
    if arg1 is None or arg2 is None:
        return None
    return _jaccard(None, arg1, arg2)


def jpack(input):
    if input is None:
        return None
    return _jpack(None, input)


def jsoncount(jval):
    if jval is None:
        return None
    return _jsoncount(None, jval)


def jsonparse(json_content, key1):
    if json_content is None or key1 is None:
        return None
    return _jsonparse(None, json_content, key1)


def jsort(jval):
    if jval is None:
        return "[]"
    return _jsort(None, jval)


def jsortvalues(jval):
    if jval is None:
        return "[]"
    return _jsortvalues(None, jval)


def keywords(input):
    if input is None:
        return None
    return _keywords(None, input)


def log_10(x):
    if x is None:
        return 0.0
    return _log_10(None, x)


def lowerize(val):
    if val is None:
        return None
    return _lowerize(None, val)


def removeshortterms(jval):
    if jval is None:
        return "[]"
    return _removeshortterms(None, jval)


def stem(input):
    if input is None:
        return None
    return _stem(None, input)
