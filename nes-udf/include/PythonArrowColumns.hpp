/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

namespace NES
{

/// Converts between pyarrow columns and the Python values of UDF arguments and results, for the Python UDFs that run on Arrow
/// batches: VARSIZED is `binary` and CHAR `fixed_size_binary(1)`, offered as bytes to a bytes parameter (see
/// PythonBytesParametersSource) and as str, failing on invalid UTF-8, to any other. Expects pyarrow imported as `pa`.
///  - _binary_column / _char_column: the values of a VARSIZED / CHAR column as a list.
///  - _binary / _char: a VARSIZED / CHAR result as bytes, raising for anything but bytes or str (of one byte for CHAR).
constexpr const char* PythonArrowColumnsSource = R"(

def _binary_column(column, as_bytes):
    # Casting to string validates UTF-8.
    return column.to_pylist() if as_bytes else column.cast(pa.string()).to_pylist()


def _char_column(column, as_bytes):
    # Decoding a single byte as UTF-8 fails for anything but ASCII.
    values = column.to_pylist()
    return values if as_bytes else [None if v is None else v.decode() for v in values]


def _char(value):
    value = _binary(value)
    if len(value) != 1:
        raise ValueError("a CHAR UDF must return one byte, not " + str(len(value)))
    return value


def _binary(value):
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("a VARSIZED UDF must return bytes or str, not " + type(value).__name__)


)";

}
