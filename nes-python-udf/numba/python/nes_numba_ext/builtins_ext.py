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
"""int(str) for nopython mode, with CPython's rules for the ASCII digits: surrounding whitespace, a sign, underscores between
digits; anything else raises ValueError. Non-ASCII decimal digits, which CPython also accepts, raise ValueError here."""

from numba import types
from numba.core.extending import overload


@overload(int)
def _int_from_string(text):
    if not isinstance(text, (types.UnicodeType, types.StringLiteral)):
        return None

    def implementation(text):
        stripped = text.strip()
        if len(stripped) == 0:
            raise ValueError("invalid literal for int()")
        position = 0
        negative = False
        if stripped[0] == "+" or stripped[0] == "-":
            negative = stripped[0] == "-"
            position = 1
        if position >= len(stripped):
            raise ValueError("invalid literal for int()")
        value = 0
        previous_digit = False
        while position < len(stripped):
            code = ord(stripped[position])
            if 48 <= code <= 57:
                value = value * 10 + (code - 48)
                previous_digit = True
            elif code == 95 and previous_digit and position + 1 < len(stripped):
                previous_digit = False
            else:
                raise ValueError("invalid literal for int()")
            position += 1
        if not previous_digit:
            raise ValueError("invalid literal for int()")
        return -value if negative else value

    return implementation

