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
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <std/cstring.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{

bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    return std::ranges::any_of(*nullValues, [fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

void parseRawValueIntoRecord(
    const DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues,
    const QuotationType quotationType)
{
    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN: {
            const auto varVal = parseFixedSizeIntoVarVal<bool>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::INT8: {
            const auto varVal = parseFixedSizeIntoVarVal<int8_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::INT16: {
            const auto varVal = parseFixedSizeIntoVarVal<int16_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::INT32: {
            const auto varVal = parseFixedSizeIntoVarVal<int32_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::INT64: {
            const auto varVal = parseFixedSizeIntoVarVal<int64_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::UINT8: {
            const auto varVal = parseFixedSizeIntoVarVal<uint8_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::UINT16: {
            const auto varVal = parseFixedSizeIntoVarVal<uint16_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::UINT32: {
            const auto varVal = parseFixedSizeIntoVarVal<uint32_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::UINT64: {
            const auto varVal = parseFixedSizeIntoVarVal<uint64_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::FLOAT32: {
            const auto varVal = parseFixedSizeIntoVarVal<float>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::FLOAT64: {
            const auto varVal = parseFixedSizeIntoVarVal<double>(dataType.nullable, fieldAddress, fieldSize, nullValues);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::CHAR: {
            switch (quotationType)
            {
                case QuotationType::NONE: {
                    const auto varVal = parseFixedSizeIntoVarVal<char>(dataType.nullable, fieldAddress, fieldSize, nullValues);
                    record.write(fieldName, varVal);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto varVal = parseFixedSizeIntoVarVal<char>(
                        dataType.nullable, fieldAddress + nautilus::val<uint32_t>(1), fieldSize - nautilus::val<uint32_t>(2), nullValues);
                    record.write(fieldName, varVal);
                    return;
                }
            }
            std::unreachable();
        }
        case DataType::Type::VARSIZED: {
            nautilus::val<bool> isNull = false;
            if (dataType.nullable)
            {
                isNull = nautilus::invoke(
                    {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
                    checkIsNullProxy,
                    fieldAddress,
                    fieldSize,
                    nautilus::val<const std::vector<std::string>*>{&nullValues});
            }

            switch (quotationType)
            {
                case QuotationType::NONE: {
                    const VariableSizedData varSized{fieldAddress, fieldSize};
                    const VarVal varVal{varSized, dataType.nullable, isNull};
                    record.write(fieldName, varVal);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto fieldAddressWithoutOpeningQuote = fieldAddress + nautilus::val<uint32_t>(1);
                    const auto fieldSizeWithoutClosingQuote = fieldSize - nautilus::val<uint32_t>(2);

                    const VariableSizedData varSized{fieldAddressWithoutOpeningQuote, fieldSizeWithoutClosingQuote};
                    const VarVal varVal{varSized, dataType.nullable, isNull};
                    record.write(fieldName, varVal);
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
