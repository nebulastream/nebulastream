# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example scalar UDFs, registered via CREATE FUNCTION ... ENTRYPOINT 'currency.<fn>'; a `None` return maps to SQL NULL."""

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
    """Always returns 42 and ignores x; a NULL input must short-circuit to SQL NULL without calling this."""
    return 42


def half_if_even(n):
    """Halve an even integer; return None (=> SQL NULL) for an odd one."""
    return n // 2 if n % 2 == 0 else None


def shout(text):
    """Uppercase a VARSIZED value (bytes in, bytes out)."""
    return text.upper()


def boom(_x):
    """Always raises — used to prove a UDF failure surfaces as a query error."""
    raise ValueError("intentional Python UDF failure")


def apply_discount(price):
    """Apply a fixed 10% discount to a bid price (FLOAT64 -> FLOAT64)."""
    if price is None:
        return None
    return price * 0.9

def id_to_name(id):
    """Map each digit of an id to a letter, 0->A .. 9->J (INT32 -> VARSIZED)."""
    if id is None:
        return None
    return ''.join(chr(ord('A') + int(digit)) for digit in str(id))