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

#include <RawValueParser.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <std/cstring.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
template <typename T>
VarVal handleNullOrParse(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const nautilus::val<bool>& isNull,
    const bool isNullable)
{
    if (isNullable and isNull)
    {
        return VarVal{nautilus::val<T>{0}, VarVal::NULLABLE_ENUM::NULLABLE, true};
    }
    return VarVal{
        parseIntoNautilusRecord<T>(fieldAddress, fieldSize),
        isNullable ? VarVal::NULLABLE_ENUM::NULLABLE : VarVal::NULLABLE_ENUM::NOT_NULLABLE,
        isNull};
}
}

void parseRawValueIntoRecord(
    const DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues,
    const QuotationType quotationType,
    ArenaRef& arenaRef)
{
    nautilus::val<bool> isNull = false;
    if (dataType.isNullable)
    {
        for (const auto& nullValue : nautilus::static_iterable(nullValues))
        {
            if (fieldSize == nullValue.length())
            {
                nautilus::val<int8_t*> fieldIterator = fieldAddress;
                for (nautilus::static_val<uint64_t> i = 0; i < nullValue.length(); ++i)
                {
                    if (fieldIterator[i] != nautilus::val<int8_t>{static_cast<int8_t>(nullValue[i])})
                    {
                        isNull = isNull and false;
                    }
                }
                isNull = true;
            }
        }
    }

    switch (dataType.type)
    {
        case DataType::Type::INT8: {
            record.write(fieldName, handleNullOrParse<int8_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::INT16: {
            record.write(fieldName, handleNullOrParse<int16_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::INT32: {
            record.write(fieldName, handleNullOrParse<int32_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::INT64: {
            record.write(fieldName, handleNullOrParse<int64_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT8: {
            record.write(fieldName, handleNullOrParse<uint8_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT16: {
            record.write(fieldName, handleNullOrParse<uint16_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT32: {
            record.write(fieldName, handleNullOrParse<uint32_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::UINT64: {
            record.write(fieldName, handleNullOrParse<uint64_t>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::FLOAT32: {
            record.write(fieldName, handleNullOrParse<float>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::FLOAT64: {
            record.write(fieldName, handleNullOrParse<double>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::CHAR: {
            switch (quotationType)
            {
                case QuotationType::NONE: {
                    record.write(fieldName, handleNullOrParse<char>(fieldAddress, fieldSize, isNull, dataType.isNullable));
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    record.write(
                        fieldName,
                        handleNullOrParse<char>(
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
            record.write(fieldName, handleNullOrParse<bool>(fieldAddress, fieldSize, isNull, dataType.isNullable));
            return;
        }
        case DataType::Type::VARSIZED: {
            switch (quotationType)
            {
                case QuotationType::NONE: {
                    const auto nullableEnum = dataType.isNullable ? VarVal::NULLABLE_ENUM::NULLABLE : VarVal::NULLABLE_ENUM::NOT_NULLABLE;
                    const auto varSizedSized = isNull ? 0 : fieldSize;
                    const auto varSized = arenaRef.allocateVariableSizedData(varSizedSized);
                    nautilus::memcpy(varSized.getContent(), fieldAddress, fieldSize);
                    record.write(fieldName, VarVal{varSized, nullableEnum, isNull});
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto nullableEnum = dataType.isNullable ? VarVal::NULLABLE_ENUM::NULLABLE : VarVal::NULLABLE_ENUM::NOT_NULLABLE;
                    const auto fieldAddressWithoutOpeningQuote = fieldAddress + nautilus::val<uint32_t>(1);
                    const auto fieldSizeWithoutClosingQuote = isNull ? 0 : fieldSize - nautilus::val<uint32_t>(2);
                    const auto varSized = arenaRef.allocateVariableSizedData(fieldSizeWithoutClosingQuote);
                    nautilus::memcpy(varSized.getContent(), fieldAddressWithoutOpeningQuote, fieldSizeWithoutClosingQuote);
                    record.write(fieldName, VarVal{varSized, nullableEnum, isNull});
                    return;
                }
            }
            std::unreachable();
        }
        case DataType::Type::VARSIZED_POINTER_REP:
            throw NotImplemented("Cannot parse varsized pointer rep type.");
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}
}
