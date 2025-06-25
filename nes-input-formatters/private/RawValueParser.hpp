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

#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include "Nautilus/Interface/Record.hpp"

namespace NES::InputFormatters::RawValueParser
{

enum class QuotationType : uint8_t
{
    NONE,
    DOUBLE_QUOTE
};

using ParseFunctionSignature = std::function<void(std::string_view inputString)>;


/// Takes a target integer type and an integer value represented as a string. Attempts to parse the string to a C++ integer of the target type.
/// @Note throws CannotFormatMalformedStringValue if the parsing fails.
/// @Note given a string like '0751' and an integer value, from_chars creates an integer '751' from it. Also, '0.751' becomes '0'.
template <typename T>
auto parseFieldString()
{
    return [](const std::string_view fieldValueString) { return NES::Util::from_chars_with_exception<T>(fieldValueString); };
}

template <typename T>
auto parseQuotedFieldString()
{
    return [](const std::string_view quotedFieldValueString)
    {
        INVARIANT(quotedFieldValueString.length() >= 2, "Input string must be at least 2 characters long.");
        const auto fieldValueString = quotedFieldValueString.substr(1, quotedFieldValueString.length() - 2);
        return NES::Util::from_chars_with_exception<T>(fieldValueString);
    };
}

Nautilus::VariableSizedData parseVarSizedIntoNautilusRecord(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const QuotationType quotationType);

template <typename T>
nautilus::val<T> parseIntoNautilusRecord(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const QuotationType quotationType)
{
    switch (quotationType)
    {
        case QuotationType::NONE: {
            return nautilus::invoke(
                +[](const char* fieldAddress, const uint64_t fieldSize)
                {
                    const auto fieldView = std::string_view(fieldAddress, fieldSize);
                    return NES::Util::from_chars_with_exception<T>(fieldView);
                },
                fieldAddress,
                fieldSize);
        }
        case QuotationType::DOUBLE_QUOTE: {
            return nautilus::invoke(
                +[](const char* fieldAddress, const uint64_t fieldSize)
                {
                    INVARIANT(fieldSize >= 2, "Input string must be at least 2 characters long.");
                    const auto fieldView = std::string_view(fieldAddress + 1, fieldSize - 1);
                    return NES::Util::from_chars_with_exception<T>(fieldView);
                },
                fieldAddress,
                fieldSize);
        }
    }
}

void parseRawValueIntoRecord(
    DataType::Type physicalType,
    Nautilus::Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    QuotationType quotationType);

/// Takes a vector containing parse function for fields. Adds a parse function that parses strings to the vector.
ParseFunctionSignature getParseFunction(DataType::Type physicalType, QuotationType quotationType);
}
