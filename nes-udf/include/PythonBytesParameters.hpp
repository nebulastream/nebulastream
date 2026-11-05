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

/// Defines _bytes_parameters(function, count), which tells for each of the first `count` parameters of a Python UDF if it
/// is annotated as bytes, also as part of a union such as Optional[bytes]. NES has one VARSIZED type for text and binary
/// data until it distinguishes them itself, so these annotations choose: a bytes parameter receives the raw bytes, and
/// any other parameter receives a str and fails on invalid UTF-8. Without usable annotations every parameter is a str.
constexpr const char* PythonBytesParametersSource = R"(
import inspect
import typing


def _bytes_parameters(function, count):
    flags = [False] * count
    try:
        hints = typing.get_type_hints(function)
        names = list(inspect.signature(function).parameters)
    except Exception:
        return flags
    for index, name in enumerate(names[:count]):
        hint = hints.get(name)
        flags[index] = hint is bytes or bytes in typing.get_args(hint)
    return flags
)";

}
