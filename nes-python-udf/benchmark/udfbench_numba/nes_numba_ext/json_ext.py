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
"""json.loads and json.dumps for nopython mode, for JSON arrays of strings (the json plugin of NES's Codon path has the same
limit). json.loads of anything else raises ValueError. json.dumps writes what CPython's json.dumps writes with its defaults:
", " between elements and non-ASCII characters as \\uXXXX (surrogate pairs above U+FFFF)."""

import json

from numba import types
from numba.core.extending import overload, register_jitable

_HEX = "0123456789abcdef"


@register_jitable
def _is_space(character):
    return character == " " or character == "\t" or character == "\n" or character == "\r"


@register_jitable
def _skip_space(text, position):
    while position < len(text) and _is_space(text[position]):
        position += 1
    return position


@register_jitable
def _hex_value(text, position):
    if position + 4 > len(text):
        raise ValueError("Invalid \\uXXXX escape")
    value = 0
    for offset in range(4):
        code = ord(text[position + offset])
        if 48 <= code <= 57:
            digit = code - 48
        elif 97 <= code <= 102:
            digit = code - 87
        elif 65 <= code <= 70:
            digit = code - 55
        else:
            raise ValueError("Invalid \\uXXXX escape")
        value = value * 16 + digit
    return value


@register_jitable
def _parse_string(text, position):
    """The string that starts after the quote at `position`, and the position after its closing quote."""
    parts = [""]
    start = position
    while True:
        if position >= len(text):
            raise ValueError("Unterminated string")
        character = text[position]
        if character == '"':
            parts.append(text[start:position])
            return "".join(parts), position + 1
        if character == "\\":
            parts.append(text[start:position])
            if position + 1 >= len(text):
                raise ValueError("Unterminated string")
            escape = text[position + 1]
            position += 2
            if escape == '"' or escape == "\\" or escape == "/":
                parts.append(escape)
            elif escape == "b":
                parts.append("\b")
            elif escape == "f":
                parts.append("\f")
            elif escape == "n":
                parts.append("\n")
            elif escape == "r":
                parts.append("\r")
            elif escape == "t":
                parts.append("\t")
            elif escape == "u":
                code = _hex_value(text, position)
                position += 4
                if 0xD800 <= code <= 0xDBFF and position + 6 <= len(text) and text[position] == "\\" and text[position + 1] == "u":
                    low = _hex_value(text, position + 2)
                    if 0xDC00 <= low <= 0xDFFF:
                        code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)
                        position += 6
                parts.append(chr(code))
            else:
                raise ValueError("Invalid escape")
            start = position
            continue
        if ord(character) < 0x20:
            raise ValueError("Invalid control character")
        position += 1


@register_jitable
def _loads_string_list(text):
    position = _skip_space(text, 0)
    if position >= len(text) or text[position] != "[":
        raise ValueError("Expected a JSON array of strings")
    result = [""]
    result.pop()
    position = _skip_space(text, position + 1)
    if position < len(text) and text[position] == "]":
        position += 1
    else:
        while True:
            if position >= len(text) or text[position] != '"':
                raise ValueError("Expected a JSON array of strings")
            value, position = _parse_string(text, position + 1)
            result.append(value)
            position = _skip_space(text, position)
            if position < len(text) and text[position] == ",":
                position = _skip_space(text, position + 1)
                continue
            if position < len(text) and text[position] == "]":
                position += 1
                break
            raise ValueError("Expected , or ]")
    if _skip_space(text, position) != len(text):
        raise ValueError("Extra data")
    return result


@register_jitable
def _escape_code(parts, code):
    parts.append("\\u")
    parts.append(_HEX[(code >> 12) & 15])
    parts.append(_HEX[(code >> 8) & 15])
    parts.append(_HEX[(code >> 4) & 15])
    parts.append(_HEX[code & 15])


@register_jitable
def _dump_string(parts, value):
    parts.append('"')
    start = 0
    for position in range(len(value)):
        character = value[position]
        code = ord(character)
        if code >= 0x20 and code < 0x7F and character != '"' and character != "\\":
            continue
        parts.append(value[start:position])
        start = position + 1
        if character == '"':
            parts.append('\\"')
        elif character == "\\":
            parts.append("\\\\")
        elif character == "\n":
            parts.append("\\n")
        elif character == "\r":
            parts.append("\\r")
        elif character == "\t":
            parts.append("\\t")
        elif character == "\b":
            parts.append("\\b")
        elif character == "\f":
            parts.append("\\f")
        elif code == 0x7F:
            parts.append(character)
        elif code > 0xFFFF:
            code -= 0x10000
            _escape_code(parts, 0xD800 | ((code >> 10) & 0x3FF))
            _escape_code(parts, 0xDC00 | (code & 0x3FF))
        else:
            _escape_code(parts, code)
    parts.append(value[start:])
    parts.append('"')


@overload(json.loads)
def _loads(text):
    if isinstance(text, (types.UnicodeType, types.StringLiteral)):
        def implementation(text):
            return _loads_string_list(text)
        return implementation
    return None


@overload(json.dumps)
def _dumps(values):
    if isinstance(values, (types.List, types.ListType)) and isinstance(values.dtype, (types.UnicodeType, types.StringLiteral)):
        def implementation(values):
            parts = ["["]
            for index in range(len(values)):
                if index > 0:
                    parts.append(", ")
                _dump_string(parts, values[index])
            parts.append("]")
            return "".join(parts)
        return implementation
    return None
