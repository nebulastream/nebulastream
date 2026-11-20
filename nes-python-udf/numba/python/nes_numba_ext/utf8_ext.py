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
"""UTF-8 text in and out of Numba strings, for UDF entries that take and return UTF-8 bytes (NES's VARSIZED values).

Numba's string type stores code points in one, two or four bytes each (CPython's compact form), not UTF-8. For ASCII text the
two are the same bytes: text_from_utf8 returns a string that points at the given bytes (one byte per code point, no owner,
like Numba's constant strings), and decodes only text that is not ASCII. write_utf8 copies an ASCII string as it is and
encodes any other. utf8_size gives the number of bytes write_utf8 writes.
"""

import numba
from numba import types
from numba.core import cgutils
from numba.core.extending import intrinsic, register_jitable
from numba.core.imputils import impl_ret_borrowed
from numba.cpython.unicode import _empty_string, _get_code_point, _set_code_point


@intrinsic
def _ascii_view(typingctx, data, length):
    """A string over `length` ASCII bytes at `data`, without a copy and without an owner."""
    def codegen(context, builder, signature, arguments):
        text = cgutils.create_struct_proxy(types.unicode_type)(context, builder)
        text.data = builder.bitcast(arguments[0], text.data.type)
        text.length = arguments[1]
        text.kind = text.kind.type(1)
        text.is_ascii = text.is_ascii.type(1)
        text.hash = text.hash.type(-1)
        return text._getvalue()
    return types.unicode_type(data, length), codegen


@intrinsic
def _copy_bytes(typingctx, destination, text):
    """Copies the bytes of an ASCII (one byte per code point) string to `destination`."""
    def codegen(context, builder, signature, arguments):
        string = cgutils.create_struct_proxy(types.unicode_type)(context, builder, value=arguments[1])
        target = builder.bitcast(arguments[0], cgutils.voidptr_t)
        source = builder.bitcast(string.data, cgutils.voidptr_t)
        cgutils.raw_memcpy(builder, target, source, string.length, 1)
        return context.get_dummy_value()
    return types.none(destination, text), codegen


@intrinsic
def unwrap(typingctx, value):
    """The value of an Optional that is known not to be None (and any other value as it is)."""
    # The result is a new reference, as Numba releases both the argument and the result: without the incref, the UDF's
    # result is released twice, and the second release writes into freed memory.
    if isinstance(value, types.Optional):
        def codegen(context, builder, signature, arguments):
            result = context.cast(builder, arguments[0], signature.args[0], signature.return_type)
            return impl_ret_borrowed(context, builder, signature.return_type, result)
        return value.type(value), codegen

    def same(context, builder, signature, arguments):
        return impl_ret_borrowed(context, builder, signature.return_type, arguments[0])
    return value(value), same


@register_jitable
def _next(data, i):
    b = data[i]
    if b < 0x80:
        return b, i + 1
    if b < 0xE0:
        return ((b & 0x1F) << 6) | (data[i + 1] & 0x3F), i + 2
    if b < 0xF0:
        return ((b & 0x0F) << 12) | ((data[i + 1] & 0x3F) << 6) | (data[i + 2] & 0x3F), i + 3
    return ((b & 0x07) << 18) | ((data[i + 1] & 0x3F) << 12) | ((data[i + 2] & 0x3F) << 6) | (data[i + 3] & 0x3F), i + 4


@register_jitable
def text_from_utf8(pointer, length):
    """A Numba string from `length` UTF-8 bytes at `pointer` (a CPointer(uint8))."""
    data = numba.carray(pointer, length)
    ascii = True
    for i in range(length):
        if data[i] >= 0x80:
            ascii = False
            break
    if ascii:
        return _ascii_view(pointer, length)
    count, largest, i = 0, 0, 0
    while i < length:
        code, i = _next(data, i)
        count += 1
        largest = max(largest, code)
    result = _empty_string(1 if largest < 256 else (2 if largest < 65536 else 4), count, False)
    i, j = 0, 0
    while i < length:
        code, i = _next(data, i)
        _set_code_point(result, j, code)
        j += 1
    return result


@register_jitable
def utf8_size(text):
    if text._is_ascii:
        return len(text)
    size = 0
    for j in range(len(text)):
        code = _get_code_point(text, j)
        size += 1 if code < 0x80 else (2 if code < 0x800 else (3 if code < 0x10000 else 4))
    return size


@register_jitable
def write_utf8(text, pointer):
    """Writes `text` as UTF-8 to `pointer` (room for utf8_size(text) bytes); returns the number of bytes."""
    if text._is_ascii:
        _copy_bytes(pointer, text)
        return len(text)
    out = numba.carray(pointer, utf8_size(text))
    n = 0
    for j in range(len(text)):
        code = _get_code_point(text, j)
        if code < 0x80:
            out[n] = code
            n += 1
        elif code < 0x800:
            out[n] = 0xC0 | (code >> 6)
            out[n + 1] = 0x80 | (code & 0x3F)
            n += 2
        elif code < 0x10000:
            out[n] = 0xE0 | (code >> 12)
            out[n + 1] = 0x80 | ((code >> 6) & 0x3F)
            out[n + 2] = 0x80 | (code & 0x3F)
            n += 3
        else:
            out[n] = 0xF0 | (code >> 18)
            out[n + 1] = 0x80 | ((code >> 12) & 0x3F)
            out[n + 2] = 0x80 | ((code >> 6) & 0x3F)
            out[n + 3] = 0x80 | (code & 0x3F)
            n += 4
    return n
