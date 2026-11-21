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
"""UDFBench log_10 for LPython. Changes: LPython types, no `self`, no try/except (math.log10 raises ValueError for
x <= 0, which gives 0.0, here an explicit check)."""
from lpython import ccallable, f64
from math import log10


def log_10(x: f64) -> f64:
    if x <= 0.0:
        return 0.0
    return log10(x)


@ccallable
def udf_entry(x: f64) -> f64:
    return log_10(x)
