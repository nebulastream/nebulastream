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
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

enum class QuotationType : uint8_t
{
    NONE,
    DOUBLE_QUOTE
};

template <typename T>
struct ParseResult
{
    T value;
    bool isNull;
};

void parseRawValueIntoRecord(
    DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues,
    QuotationType quotationType);

/// We expect a pointer and the size so that we can use this method from the nautilus runtime
bool checkIsNullProxy(const int8_t* fieldAddress, uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept;

template <typename T, bool Nullable>
requires(
    not(std::is_same_v<T, int8_t*> || std::is_same_v<T, uint8_t*> || std::is_same_v<T, std::byte*> || std::is_same_v<T, char*>
        || std::is_same_v<T, unsigned char*> || std::is_same_v<T, signed char*>))
ParseResult<T>* parseIntoVarValProxy(int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static ParseResult<T> result;
    result.isNull = false;

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
        {
            result.isNull = true;
            result.value = T{0};
            return &result;
        }
    }

    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    result.value = NES::from_chars_with_exception<T>(fieldAsString);
    return &result;
}

template <typename T>
VarVal parseFixedSizeIntoVarVal(
    const bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues)
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            parseIntoVarValProxy<T, true>, fieldAddress, fieldSize, nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResult<T>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<T>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        parseIntoVarValProxy<T, false>, fieldAddress, fieldSize, nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResult<T>, value));
    return VarVal{nautilusValue, nullable, false};
}
}
