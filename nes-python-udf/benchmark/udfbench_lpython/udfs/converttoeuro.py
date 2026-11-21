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
"""UDFBench converttoeuro for LPython. Changes: LPython types, no `self`, no try/except (a KeyError gives 0.0, here
an explicit check). The dict is built in each call, as in the original."""
from lpython import ccallable, f64


def converttoeuro(x: f64, y: str) -> f64:
    euro_equals: dict[str, f64] = {
        "EUR": 1.00,
        "": 1.00,
        "NOK": 11.59,
        "AUD": 1.63,
        "CAD": 1.44,
        "$": 1.09,
        "USD": 1.09,
        "GBP": 0.85,
        "CHF": 0.98,
        "ZAR": 20.41,
        "SGD": 1.47,
        "INR": 89.61,
    }
    if y in euro_equals:
        return x / euro_equals[y]
    return 0.0


@ccallable
def udf_entry(x: f64, y: str) -> f64:
    amount: f64 = x  # LPython miscompiles a @ccallable function that passes its arguments on
    copy: str = y
    return converttoeuro(amount, copy)
