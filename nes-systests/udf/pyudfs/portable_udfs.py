# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Scalar UDFs written in the subset of Python that CPython, PyPy and Codon all accept, so a single module can back
`CREATE FUNCTION ... BRIDGE 'cpython' | 'pypy' | 'codon'` (see PythonUdfBridgeComparison.test and CodonUdf.test).

A NULL argument never reaches these functions: UDF calls are strict, so the result is NULL without a call.
"""


def apply_discount(price):
    """Apply a fixed 10% discount to a bid price (FLOAT64 -> FLOAT64)."""
    return price * 0.9


def add(a, b):
    """Add two integers."""
    return a + b


def shout(text):
    """Uppercase a VARSIZED value."""
    return text.upper()
