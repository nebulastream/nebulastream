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
"""UDFBench lowerize for LPython. Changes: LPython types, no `self`, no try/except, None is "NULL"."""
from lpython import ccallable


def lowerize(val: str) -> str:
    if val != "":
        return val.lower()
    else:
        return "NULL"


@ccallable
def udf_entry(val: str) -> str:
    copy: str = val  # LPython miscompiles a @ccallable function that passes its str argument on
    return lowerize(copy)
