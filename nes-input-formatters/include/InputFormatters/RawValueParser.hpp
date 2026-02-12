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
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <std/cstring.h>
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
    bool validValue;
};

template <typename T>
ParseResult<T>* parseIntoNautilusRecordProxy(const char* fieldAddress, const uint64_t fieldSize, const bool isNull)
{
    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static ParseResult<T> result;

    /// We handle null in the nautilus::invoke to reduce the amount of branches in nautilus code to reduce the tracing time.
    if (isNull)
    {
        result.validValue = false;
        result.value = T{0};
        return &result;
    }

    const std::string_view fieldView{fieldAddress, fieldSize};
    try
    {
        const auto value = NES::from_chars_with_exception<T>(fieldView);
        result.validValue = true;
        result.value = value;
    }
    catch (...)
    {
        result.validValue = false;
        result.value = T{0};
    }
    return &result;
}

template <typename T>
VarVal parseIntoNautilusRecord(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const nautilus::val<bool>& isNull,
    const DataType::NULLABLE isNullable)
{
    const auto parseResult = nautilus::invoke(parseIntoNautilusRecordProxy<T>, fieldAddress, fieldSize, isNull);
    const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResult<T>, value));
    const nautilus::val<bool> isValidValue = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<T>, validValue));
    return VarVal{nautilusValue, isNullable, isNull or not isValidValue};
}

template <typename IndexerMetaData>
bool isNullProxy(int8_t* fieldAddress, const uint64_t fieldSize, const IndexerMetaData* metaData)
{
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    for (const auto& nullValue : metaData->getNullValues())
    {
        if (nullValue == fieldAsString)
        {
            return true;
        }
    }
    return false;
}

template <typename IndexerMetaData>
void parseRawValueIntoRecord(
    DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const IndexerMetaData& metaData,
    ArenaRef& arenaRef)
{
    nautilus::val<bool> isNull = false;
    if (dataType.isNullableAsBool())
    {
        isNull = nautilus::invoke(isNullProxy<IndexerMetaData>, fieldAddress, fieldSize, nautilus::val<const IndexerMetaData*>{&metaData});
    }
    switch (dataType.type)
    {
        case DataType::Type::INT8: {
            record.write(fieldName, parseIntoNautilusRecord<int8_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::INT16: {
            record.write(fieldName, parseIntoNautilusRecord<int16_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::INT32: {
            record.write(fieldName, parseIntoNautilusRecord<int32_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::INT64: {
            record.write(fieldName, parseIntoNautilusRecord<int64_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT8: {
            record.write(fieldName, parseIntoNautilusRecord<uint8_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT16: {
            record.write(fieldName, parseIntoNautilusRecord<uint16_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT32: {
            record.write(fieldName, parseIntoNautilusRecord<uint32_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT64: {
            record.write(fieldName, parseIntoNautilusRecord<uint64_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::FLOAT32: {
            record.write(fieldName, parseIntoNautilusRecord<float>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::FLOAT64: {
            record.write(fieldName, parseIntoNautilusRecord<double>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::CHAR: {
            switch (metaData.getQuotationType())
            {
                case QuotationType::NONE: {
                    record.write(fieldName, parseIntoNautilusRecord<char>(fieldAddress, fieldSize, isNull, dataType.isNullable));
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    record.write(
                        fieldName,
                        parseIntoNautilusRecord<char>(
                            fieldAddress + nautilus::val<uint32_t>(1),
                            fieldSize - nautilus::val<uint32_t>(2),
                            isNull,
                            dataType.isNullable));
                    return;
                }
            }
            std::unreachable();
        }
        case DataType::Type::BOOLEAN: {
            record.write(fieldName, parseIntoNautilusRecord<bool>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::VARSIZED: {
            switch (metaData.getQuotationType())
            {
                case QuotationType::NONE: {
                    const auto varSizedSized = isNull ? 0 : fieldSize;
                    const auto varSized = arenaRef.allocateVariableSizedData(varSizedSized);
                    nautilus::memcpy(varSized.getContent(), fieldAddress, fieldSize);
                    record.write(fieldName, VarVal{varSized, dataType.isNullable, isNull});
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto fieldAddressWithoutOpeningQuote = fieldAddress + nautilus::val<uint32_t>(1);
                    const auto fieldSizeWithoutClosingQuote = isNull ? 0 : fieldSize - nautilus::val<uint32_t>(2);
                    const auto varSized = arenaRef.allocateVariableSizedData(fieldSizeWithoutClosingQuote);
                    nautilus::memcpy(varSized.getContent(), fieldAddressWithoutOpeningQuote, fieldSizeWithoutClosingQuote);
                    record.write(fieldName, VarVal{varSized, dataType.isNullable, isNull});
                    return;
                }
            }
            std::unreachable();
        }
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}
}
