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
#include <Util/Strings.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::InputFormatters::RawInputDataParser
{

using ParseFunctionSignature = std::function<void(
    std::string_view fieldValue,
    int8_t* writeAddress,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& bufferFormatted)>;

struct FieldConfig
{
    size_t fieldSize;
    ParseFunctionSignature parseFunction;
};

/// Takes a target integer type and an integer value represented as a string. Attempts to parse the string to a C++ integer of the target type.
/// @Note throws CannotFormatMalformedStringValue if the parsing fails.
/// @Note given a string like '0751' and an integer value, from_chars creates an integer '751' from it. Also, '0.751' becomes '0'.
template <typename T, bool Nullable>
ParseFunctionSignature parseFieldString(const Sources::ParserConfig& config)
{
    if constexpr (Nullable)
    {
        return [&config](const std::string_view fieldValue, int8_t* writeAddress, Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
        {
            /// Check if the nullable field is actually null
            /// Either the value is equal to the null representation
            /// Or the value is equal to the field delimiter which indicates that the field was found empty during indexing
            if (fieldValue == config.nullRepresentation)
            {
                *(writeAddress + sizeof(T)) = 1;
                return;
            }
            auto* value = reinterpret_cast<T*>(writeAddress); ///NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            *value = Util::from_chars_with_exception<T>(fieldValue);
            *(writeAddress + sizeof(T)) = 0;
        };
    }
    return [](const std::string_view fieldValue, int8_t* writeAddress, Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
    {
        auto* value = reinterpret_cast<T*>(writeAddress);
        *value = Util::from_chars_with_exception<T>(fieldValue);
    };
}

/// Takes a vector containing parse function for fields. Adds a parse function that parses strings to the vector.
ParseFunctionSignature getBasicStringParseFunction(const Sources::ParserConfig& parserConfig, bool nullable);

/// Takes a vector containing parse function for fields. Adds a parse function that parses a basic NebulaStream type to the vector.
ParseFunctionSignature
getBasicTypeParseFunction(const BasicPhysicalType& basicPhysicalType, const Sources::ParserConfig& parserConfig, bool nullable);
}
