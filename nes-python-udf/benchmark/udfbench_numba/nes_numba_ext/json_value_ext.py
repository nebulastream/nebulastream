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
"""A JSON value for nopython mode, with the interface of the Value of NES's Codon json plugin: len(), [int] for an array, [str],
get() and `in` for an object, str(), and isinstance(value, list) (also dict, str, int, float, bool), which checks the kind at
run time.

get() returns a value also for a missing member, which acts as None (str() "None"): Numba cannot unify a structref
with None. result_text(value) is what NES's entry code does with a VARSIZED result (__nes_str__ in the Codon plugin):
(True, the text) for a string, (True, None) for null or a missing member, and (False, None) for any other kind, for which
the caller raises TypeError outside the UDF, so that the UDF's own `except` does not catch it. Any other value v gives
(True, v).

json_value(text) parses `text` into flat arrays of nodes. A value is the arrays and the index of a node, so a member or an
element shares the arrays of its document. The front end (frontend.njit) calls json_value in place of json.loads in a UDF that
asks a value for its kind (isinstance(x, list) or dict): one Numba type cannot be both the list of strings that json.loads gives
the other UDFs and a value of any kind.

str() gives the text of a string, None, True or False, and the JSON text of a number (CPython can write a float another way,
for example 1e5 as 100000.0). str() of an array or an object raises ValueError.
"""

import operator

import numpy
from numba import types
from numba.core.extending import overload, overload_method, register_jitable
from numba.experimental import structref
from numba.typed import List

from nes_numba_ext.json_ext import _parse_string, _skip_space

NULL, BOOL, INT, FLOAT, STRING, ARRAY, OBJECT = 0, 1, 2, 3, 4, 5, 6


@structref.register
class JsonValueType(types.StructRef):
    pass


class JsonValue(structref.StructRefProxy):
    pass


# kinds, starts, ends: the kind of each node and where its text is (for a string: its index in `strings`). firsts, nexts: the
# first child of an array or object and the next sibling. keys: for a member of an object, the node of its key, else -1.
structref.define_proxy(JsonValue, JsonValueType, ["text", "kinds", "starts", "ends", "firsts", "nexts", "keys", "strings", "index"])


@register_jitable
def _kind(value):
    """The kind of `value`; a missing member (index -1, from get) is NULL, as dict.get gives None for it."""
    if value.index < 0:
        return NULL
    return value.kinds[value.index]


@register_jitable
def _at(value, index):
    return JsonValue(value.text, value.kinds, value.starts, value.ends, value.firsts, value.nexts, value.keys, value.strings, index)


@register_jitable
def _add(nodes, kind, start, end, key):
    kinds, starts, ends, firsts, nexts, keys = nodes
    kinds.append(kind)
    starts.append(start)
    ends.append(end)
    firsts.append(-1)
    nexts.append(-1)
    keys.append(key)
    return len(kinds) - 1


@register_jitable
def _link(nodes, parents, lasts, node):
    if len(parents) == 0:
        return
    firsts, nexts = nodes[3], nodes[4]
    if lasts[-1] < 0:
        firsts[parents[-1]] = node
    else:
        nexts[lasts[-1]] = node
    lasts[-1] = node


@register_jitable
def _key(text, position, nodes, strings):
    """Parses the key of an object member at `position` and the colon after it; returns its node and the next position."""
    if position >= len(text) or text[position] != '"':
        raise ValueError("Expecting property name enclosed in double quotes")
    key, position = _parse_string(text, position + 1)
    strings.append(key)
    node = _add(nodes, STRING, len(strings) - 1, len(strings) - 1, -1)
    position = _skip_space(text, position)
    if position >= len(text) or text[position] != ":":
        raise ValueError("Expecting ':' delimiter")
    return node, _skip_space(text, position + 1)


@register_jitable
def _number_end(text, position):
    is_float = False
    while position < len(text):
        character = text[position]
        if character == "." or character == "e" or character == "E":
            is_float = True
        elif not (character == "-" or character == "+" or ("0" <= character <= "9")):
            break
        position += 1
    return position, is_float


@register_jitable
def json_value(text):
    zero = [0]
    zero.pop()
    nodes = (list(zero), list(zero), list(zero), list(zero), list(zero), list(zero))
    strings = List.empty_list(types.unicode_type)
    parents = list(zero)
    lasts = list(zero)
    position = _skip_space(text, 0)
    key = -1
    expect_value = True
    while True:
        if expect_value:
            if position >= len(text):
                raise ValueError("Expecting value")
            character = text[position]
            if character == "[" or character == "{":
                node = _add(nodes, ARRAY if character == "[" else OBJECT, position, position, key)
                _link(nodes, parents, lasts, node)
                parents.append(node)
                lasts.append(-1)
                position = _skip_space(text, position + 1)
                closing = "]" if character == "[" else "}"
                if position < len(text) and text[position] == closing:
                    parents.pop()
                    lasts.pop()
                    position += 1
                    expect_value = False
                elif character == "{":
                    key, position = _key(text, position, nodes, strings)
                else:
                    key = -1
                continue
            if character == '"':
                value, end = _parse_string(text, position + 1)
                strings.append(value)
                node = _add(nodes, STRING, len(strings) - 1, len(strings) - 1, key)
                position = end
            elif text.startswith("null", position):
                node = _add(nodes, NULL, position, position + 4, key)
                position += 4
            elif text.startswith("true", position):
                node = _add(nodes, BOOL, position, position + 4, key)
                position += 4
            elif text.startswith("false", position):
                node = _add(nodes, BOOL, position, position + 5, key)
                position += 5
            else:
                end, is_float = _number_end(text, position)
                if end == position:
                    raise ValueError("Expecting value")
                node = _add(nodes, FLOAT if is_float else INT, position, end, key)
                position = end
            _link(nodes, parents, lasts, node)
            expect_value = False
            continue
        position = _skip_space(text, position)
        if len(parents) == 0:
            break
        if position >= len(text):
            raise ValueError("Unterminated array or object")
        character = text[position]
        is_object = nodes[0][parents[-1]] == OBJECT
        if character == ",":
            position = _skip_space(text, position + 1)
            if is_object:
                key, position = _key(text, position, nodes, strings)
            else:
                key = -1
            expect_value = True
        elif character == ("}" if is_object else "]"):
            parents.pop()
            lasts.pop()
            position += 1
        else:
            raise ValueError("Expecting ',' delimiter")
    if position != len(text):
        raise ValueError("Extra data")
    kinds, starts, ends, firsts, nexts, keys = nodes
    return JsonValue(text, numpy.array(kinds, dtype=numpy.int64), numpy.array(starts, dtype=numpy.int64),
                     numpy.array(ends, dtype=numpy.int64), numpy.array(firsts, dtype=numpy.int64),
                     numpy.array(nexts, dtype=numpy.int64), numpy.array(keys, dtype=numpy.int64), strings, 0)


@register_jitable
def _member_node(value, key):
    child = value.firsts[value.index]
    while child >= 0:
        if value.strings[value.starts[value.keys[child]]] == key:
            return child
        child = value.nexts[child]
    return -1


@overload(len)
def _len(value):
    if isinstance(value, JsonValueType):
        def implementation(value):
            kind = _kind(value)
            if kind == STRING:
                return len(value.strings[value.starts[value.index]])
            if kind != ARRAY and kind != OBJECT:
                raise TypeError("object has no len()")
            count = 0
            child = value.firsts[value.index]
            while child >= 0:
                count += 1
                child = value.nexts[child]
            return count
        return implementation
    return None


@overload(operator.getitem)
def _getitem(value, key):
    if not isinstance(value, JsonValueType):
        return None
    if isinstance(key, types.Integer):
        def by_index(value, key):
            if _kind(value) != ARRAY:
                raise TypeError("object is not subscriptable by int")
            child = value.firsts[value.index]
            position = 0
            while child >= 0:
                if position == key:
                    return _at(value, child)
                position += 1
                child = value.nexts[child]
            raise IndexError("list index out of range")
        return by_index
    if isinstance(key, (types.UnicodeType, types.StringLiteral)):
        def by_key(value, key):
            if _kind(value) != OBJECT:
                raise TypeError("object is not subscriptable by str")
            child = _member_node(value, key)
            if child < 0:
                raise KeyError("no such member")
            return _at(value, child)
        return by_key
    return None


@overload(operator.contains)
def _contains(value, item):
    if isinstance(value, JsonValueType) and isinstance(item, (types.UnicodeType, types.StringLiteral)):
        def implementation(value, item):
            kind = _kind(value)
            if kind == OBJECT:
                return _member_node(value, item) >= 0
            if kind == STRING:
                return item in value.strings[value.starts[value.index]]
            if kind == ARRAY:
                child = value.firsts[value.index]
                while child >= 0:
                    if value.kinds[child] == STRING and value.strings[value.starts[child]] == item:
                        return True
                    child = value.nexts[child]
                return False
            raise TypeError("argument is not iterable")
        return implementation
    return None


# Numba's str() calls __str__ of a type that has one; an overload of str itself loses against its generic str of an object.
@overload_method(JsonValueType, "__str__")
def _str(value):
    if isinstance(value, JsonValueType):
        def implementation(value):
            kind = _kind(value)
            if kind == STRING:
                return value.strings[value.starts[value.index]]
            if kind == NULL:
                return "None"
            if kind == BOOL:
                return "True" if value.text[value.starts[value.index]] == "t" else "False"
            if kind == INT or kind == FLOAT:
                return value.text[value.starts[value.index]:value.ends[value.index]]
            raise ValueError("str() of a JSON array or object is not supported")
        return implementation
    return None


@overload_method(JsonValueType, "get")
def _get(value, key):
    def implementation(value, key):
        if _kind(value) != OBJECT:
            raise AttributeError("object has no attribute 'get'")
        return _at(value, _member_node(value, key))
    return implementation


_ISINSTANCE_KINDS = {list: (ARRAY,), dict: (OBJECT,), str: (STRING,), bool: (BOOL,), int: (INT, BOOL), float: (FLOAT,)}


@overload(isinstance)
def _isinstance(value, kind):
    if not isinstance(value, JsonValueType):
        return None
    target = kind.typing_key if isinstance(kind, types.Function) else getattr(kind, "instance_type", None)
    if isinstance(kind, types.NumberClass):
        target = {types.int64: int, types.float64: float, types.boolean: bool}.get(kind.instance_type)
    wanted = _ISINSTANCE_KINDS.get(target)
    if wanted is None:
        return None
    first, second = wanted[0], wanted[-1]
    return lambda value, kind: _kind(value) == first or _kind(value) == second


def result_text(value):
    """See the module docstring; Python fallback for completeness."""
    return True, value


@overload(result_text)
def _result_text(value):
    if isinstance(value, JsonValueType):
        def implementation(value):
            kind = _kind(value)
            if kind == STRING:
                return True, value.strings[value.starts[value.index]]
            if kind == NULL:
                return True, None
            return False, None
        return implementation
    return lambda value: (True, value)
