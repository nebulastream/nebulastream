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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

enum class UncompiledQuotationType : uint8_t
{
    NONE,
    DOUBLE_QUOTE
};

/// All registered Uncompiled Input Parsers need to implement a function of this signature to parse the values
/// Implementing a class is not needed. It needs to be insured, that the register<plguinname>UncompiledInputParser function returns the function
using UncompiledParseFunctionSignature = std::function<void(
    std::string_view inputString, size_t writeOffsetInBytes, AbstractBufferProvider& bufferProvider, TupleBuffer& tupleBufferFormatted)>;

}
