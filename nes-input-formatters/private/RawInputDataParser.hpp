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

#include <cstdint>
#include <string_view>
#include <functional>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::InputFormatters::RawInputDataParser
{

using ParseFunctionSignature = std::function<void(
    std::string_view inputString,
    int8_t* fieldPointer,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& tupleBufferFormatted)>;

struct FieldConfig
{
    size_t size;
    ParseFunctionSignature parseFunction;
};

/// Takes a target integer type and an integer value represented as a string. Attempts to parse the string to a C++ integer of the target type.
/// @Note throws CannotFormatMalformedStringValue if the parsing fails.
/// @Note given a string like '0751' and an integer value, from_chars creates an integer '751' from it. Also, '0.751' becomes '0'.
template <typename T>
auto parseFieldString()
{
    return [](std::string_view fieldValueString, int8_t* fieldPointer, Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
    {
        auto* value = reinterpret_cast<T*>(fieldPointer);
        *value = Util::from_chars_with_exception<T>(fieldValueString);
    };
}

/// Takes a vector containing parse function for fields. Adds a parse function that parses strings to the vector.
ParseFunctionSignature getBasicStringParseFunction();

/// Takes a vector containing parse function for fields. Adds a parse function that parses a basic NebulaStream type to the vector.
ParseFunctionSignature getBasicTypeParseFunction(const BasicPhysicalType& basicPhysicalType);
}
