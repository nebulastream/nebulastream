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
"""UDFBench extractyear for LPython. Changes: LPython types, no `self`, no try/except (int() of a bad date raises
ValueError, which gives -1; all dates of the data are well-formed)."""
from lpython import ccallable, i32, i64


def extractyear(arg: str) -> i64:
    if arg != "":
        value: i32 = i32(int(arg[:arg.find("-")]))  # LPython types int() of a str as i64, but its code gives an i32
        return i64(value)
    else:
        return i64(-1)


@ccallable
def udf_entry(arg: str) -> i64:
    copy: str = arg  # LPython miscompiles a @ccallable function that passes its str argument on
    return extractyear(copy)
