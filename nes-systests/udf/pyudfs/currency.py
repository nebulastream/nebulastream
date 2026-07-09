# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example scalar UDFs authored in plain Python.

Registered via `CREATE FUNCTION ... FROM '<python-bridge>.so' ENTRYPOINT 'currency.<fn>'`.
The bridge passes each argument as a Python object (VARSIZED arrives as `bytes`); returning
`None` maps to SQL NULL.
"""

# Test rates chosen to be exactly representable so results format cleanly.
_RATES = {b"EUR": 1.0, b"USD": 1.25, b"GBP": 0.5}


def to_euro(amount, ccy):
    """Convert an amount in a currency (VARSIZED, as bytes) to euros (FLOAT64)."""
    if amount is None or ccy is None:
        return None
    return amount / _RATES.get(ccy, 1.0)


def add(a, b):
    """Add two integers."""
    return a + b


def strict_probe(x):
    """Strict short-circuit probe: always returns the constant 42, never NULL, and never inspects x.

    Under STRICT UDF semantics a NULL argument must short-circuit to SQL NULL *without* invoking the
    UDF, so a NULL input row must yield NULL here — not 42. (If the engine wrongly invoked us, we would
    return 42, since we ignore x entirely.)
    """
    return 42


def half_if_even(n):
    """Halve an even integer; return None (=> SQL NULL) for an odd one.

    All inputs are non-NULL, so this exercises a UDF *producing* NULL via a Python `None` return,
    independent of any NULL on the input side.
    """
    return n // 2 if n % 2 == 0 else None


def shout(text):
    """Uppercase a VARSIZED value (bytes in, bytes out)."""
    return text.upper()


def boom(_x):
    """Always raises — used to prove a UDF failure surfaces as a query error."""
    raise ValueError("intentional Python UDF failure")
