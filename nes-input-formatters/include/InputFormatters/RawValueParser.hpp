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

#include <algorithm>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <nautilus/common/FunctionAttributes.hpp>
#include <Arena.hpp>
#include <val.hpp>
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
nautilus::val<T> parseIntoNautilusRecord(const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize)
{
    return nautilus::invoke(
        nautilus::FunctionAttributes{nautilus::ModRefInfo::NoModRef, true, true},
        +[](const char* fieldAddress, const uint64_t fieldSize)
        {
            const auto fieldView = std::string_view(fieldAddress, fieldSize);
            return NES::from_chars_with_exception<T>(fieldView);
        },
        fieldAddress,
        fieldSize);
}

VariableSizedData parseVarSizedIntoNautilusRecord(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, QuotationType quotationType);

void parseRawValueIntoRecord(
    DataType::Type physicalType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    QuotationType quotationType);
}
