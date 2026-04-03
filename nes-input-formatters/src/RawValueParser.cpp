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
#include <select.hpp>
#include <val.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>
#include <InputParserRegistry.hpp>


namespace NES
{

bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    return std::ranges::any_of(*nullValues, [fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

VarVal parseFixedSizeIntoVarVal(
    const bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::string& parserType)

{
    constexpr InputParserRegistryArguments args{};
    if (const auto parser = InputParserRegistry::instance().create(parserType, args))
    {
        return parser.value()->parseToVarVal(nullable, fieldAddress, fieldSize, nullValues);
    }
    throw UnknownInputParserType("Unknown Input Parser: {}", parserType);
}

void parseRawValueIntoRecord(
    const DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues,
    const QuotationType quotationType,
    const std::string& parserType)
{
    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64: {
            const auto varVal = parseFixedSizeIntoVarVal(dataType.nullable, fieldAddress, fieldSize, nullValues, parserType);
            record.write(fieldName, varVal);
            return;
        }
        case DataType::Type::CHAR: {
            switch (quotationType)
            {
                case QuotationType::NONE: {
                    const auto varVal = parseFixedSizeIntoVarVal(dataType.nullable, fieldAddress, fieldSize, nullValues, parserType);
                    record.write(fieldName, varVal);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto varVal = parseFixedSizeIntoVarVal(
                        dataType.nullable,
                        fieldAddress + nautilus::val<uint32_t>(1),
                        fieldSize - nautilus::val<uint32_t>(2),
                        nullValues,
                        parserType);
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
                    const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress);
                    const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize);
                    const VariableSizedData varSized{ptr, size};
                    const VarVal varVal{varSized, dataType.nullable, isNull};
                    record.write(fieldName, varVal);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress + nautilus::val<uint32_t>(1));
                    const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize - nautilus::val<uint64_t>(2));
                    const VariableSizedData varSized{ptr, size};
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
