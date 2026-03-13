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
#include <any>
#include <cstdint>
#include <cstring>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/RawValue.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <std/cstring.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

void parseRawValueIntoRecord(
    const DataType::Type physicalType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const QuotationType quotationType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8: {
            record.write(fieldName, parseIntoNautilusRecord<int8_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::INT16: {
            record.write(fieldName, parseIntoNautilusRecord<int16_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::INT32: {
            record.write(fieldName, parseIntoNautilusRecord<int32_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::INT64: {
            record.write(fieldName, parseIntoNautilusRecord<int64_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::UINT8: {
            record.write(fieldName, parseIntoNautilusRecord<uint8_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::UINT16: {
            record.write(fieldName, parseIntoNautilusRecord<uint16_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::UINT32: {
            record.write(fieldName, parseIntoNautilusRecord<uint32_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::UINT64: {
            record.write(fieldName, parseIntoNautilusRecord<uint64_t>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::FLOAT32: {
            record.write(fieldName, parseIntoNautilusRecord<float>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::FLOAT64: {
            record.write(fieldName, parseIntoNautilusRecord<double>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::CHAR: {
            switch (quotationType)
            {
                case QuotationType::NONE: {
                    record.write(fieldName, parseIntoNautilusRecord<char>(fieldAddress, fieldSize));
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    record.write(
                        fieldName,
                        parseIntoNautilusRecord<char>(fieldAddress + nautilus::val<uint32_t>(1), fieldSize - nautilus::val<uint32_t>(2)));
                    return;
                }
            }
            std::unreachable();
        }
        case DataType::Type::BOOLEAN: {
            record.write(fieldName, parseIntoNautilusRecord<bool>(fieldAddress, fieldSize));
            return;
        }
        case DataType::Type::VARSIZED: {
            switch (quotationType)
            {
                case QuotationType::NONE: {
                    const VariableSizedData varSized(fieldAddress, fieldSize);
                    record.write(fieldName, varSized);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto fieldAddressWithoutOpeningQuote = fieldAddress + nautilus::val<uint32_t>(1);
                    const auto fieldSizeWithoutClosingQuote = fieldSize - nautilus::val<uint32_t>(2);

                    const VariableSizedData varSized(fieldAddressWithoutOpeningQuote, fieldSizeWithoutClosingQuote);
                    record.write(fieldName, varSized);
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

/// Builds a materializer lambda that, when called, parses the raw bytes into a typed VarVal.
/// This captures the field address, size, type, and quotation type — identical to what parseRawValueIntoRecord does.
static std::any buildMaterializer(
    const DataType::Type physicalType,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const QuotationType quotationType)
{
    return std::function<VarVal()>(
        [physicalType, fieldAddress, fieldSize, quotationType]() -> VarVal
        {
            switch (physicalType)
            {
                case DataType::Type::INT8:
                    return VarVal(parseIntoNautilusRecord<int8_t>(fieldAddress, fieldSize));
                case DataType::Type::INT16:
                    return VarVal(parseIntoNautilusRecord<int16_t>(fieldAddress, fieldSize));
                case DataType::Type::INT32:
                    return VarVal(parseIntoNautilusRecord<int32_t>(fieldAddress, fieldSize));
                case DataType::Type::INT64:
                    return VarVal(parseIntoNautilusRecord<int64_t>(fieldAddress, fieldSize));
                case DataType::Type::UINT8:
                    return VarVal(parseIntoNautilusRecord<uint8_t>(fieldAddress, fieldSize));
                case DataType::Type::UINT16:
                    return VarVal(parseIntoNautilusRecord<uint16_t>(fieldAddress, fieldSize));
                case DataType::Type::UINT32:
                    return VarVal(parseIntoNautilusRecord<uint32_t>(fieldAddress, fieldSize));
                case DataType::Type::UINT64:
                    return VarVal(parseIntoNautilusRecord<uint64_t>(fieldAddress, fieldSize));
                case DataType::Type::FLOAT32:
                    return VarVal(parseIntoNautilusRecord<float>(fieldAddress, fieldSize));
                case DataType::Type::FLOAT64:
                    return VarVal(parseIntoNautilusRecord<double>(fieldAddress, fieldSize));
                case DataType::Type::BOOLEAN:
                    return VarVal(parseIntoNautilusRecord<bool>(fieldAddress, fieldSize));
                case DataType::Type::CHAR: {
                    switch (quotationType)
                    {
                        case QuotationType::NONE:
                            return VarVal(parseIntoNautilusRecord<char>(fieldAddress, fieldSize));
                        case QuotationType::DOUBLE_QUOTE:
                            return VarVal(parseIntoNautilusRecord<char>(
                                fieldAddress + nautilus::val<uint32_t>(1), fieldSize - nautilus::val<uint32_t>(2)));
                    }
                    std::unreachable();
                }
                case DataType::Type::VARSIZED:
                case DataType::Type::UNDEFINED:
                    throw NotImplemented("Cannot materialize VARSIZED or UNDEFINED type from RawValue.");
            }
            std::unreachable();
        });
}

void storeRawValueInRecord(
    const DataType::Type physicalType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const QuotationType quotationType)
{
    /// VARSIZED already does memcmp via VariableSizedData::operator==, so no benefit from RawValue.
    if (physicalType == DataType::Type::VARSIZED)
    {
        parseRawValueIntoRecord(physicalType, record, fieldAddress, fieldSize, fieldName, quotationType);
        return;
    }

    /// For CHAR with DOUBLE_QUOTE, adjust pointer/size to strip quotes before storing raw bytes.
    auto rawAddr = fieldAddress;
    auto rawSize = fieldSize;
    if (physicalType == DataType::Type::CHAR && quotationType == QuotationType::DOUBLE_QUOTE)
    {
        rawAddr = fieldAddress + nautilus::val<uint32_t>(1);
        rawSize = fieldSize - nautilus::val<uint32_t>(2);
    }

    RawValue rawValue{rawAddr, rawSize, physicalType, buildMaterializer(physicalType, fieldAddress, fieldSize, quotationType)};
    record.write(fieldName, VarVal(rawValue));
}

}
