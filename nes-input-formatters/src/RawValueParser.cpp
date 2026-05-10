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
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    return std::ranges::any_of(*nullValues, [fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

VarVal parseRawValueIntoVarVal(
    const DataType dataType,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues)
{
    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN:
            return parseFixedSizeIntoVarVal<bool>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::INT8:
            return parseFixedSizeIntoVarVal<int8_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::INT16:
            return parseFixedSizeIntoVarVal<int16_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::INT32:
            return parseFixedSizeIntoVarVal<int32_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::INT64:
            return parseFixedSizeIntoVarVal<int64_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::UINT8:
            return parseFixedSizeIntoVarVal<uint8_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::UINT16:
            return parseFixedSizeIntoVarVal<uint16_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::UINT32:
            return parseFixedSizeIntoVarVal<uint32_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::UINT64:
            return parseFixedSizeIntoVarVal<uint64_t>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::FLOAT32:
            return parseFixedSizeIntoVarVal<float>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::FLOAT64:
            return parseFixedSizeIntoVarVal<double>(dataType.nullable, fieldAddress, fieldSize, nullValues);
        case DataType::Type::CHAR:
            return parseFixedSizeIntoVarVal<char>(dataType.nullable, fieldAddress, fieldSize, nullValues);
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
            const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress);
            const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize);
            const VariableSizedData varSized{ptr, size};
            return VarVal{varSized, dataType.nullable, isNull};
        }
        case DataType::Type::FIXEDSIZED:
            throw NotImplemented("Flat textual formatters do not support FIXEDSIZED arrays.");
        case DataType::Type::STRUCT:
            throw NotImplemented("Flat textual formatters do not support STRUCT types.");
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

void parseRawValueIntoRecord(
    const DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues)
{
    record.write(fieldName, parseRawValueIntoVarVal(dataType, fieldAddress, fieldSize, nullValues));
}

}
