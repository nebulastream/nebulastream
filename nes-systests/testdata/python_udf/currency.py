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

"""Scalar UDF examples shared with the udf-dev branch compatibility tests."""

_RATES = {b"EUR": 1.0, b"USD": 1.25, b"GBP": 0.5}


def to_euro(amount, ccy):
    if amount is None or ccy is None:
        return None
    return amount / _RATES.get(ccy, 1.0)


def add(a, b):
    return a + b


def strict_probe(x):
    return 42


def half_if_even(n):
    return n // 2 if n % 2 == 0 else None


def shout(text):
    return text.upper()


def boom(_x):
    raise ValueError("intentional Python UDF failure")


def apply_discount(price):
    if price is None:
        return None
    return price * 0.9


def id_to_name(identifier):
    return "".join(chr(ord("A") + int(digit)) for digit in str(identifier))
