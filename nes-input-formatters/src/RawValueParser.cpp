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
    const QuotationType quotationType,
    ArenaRef& arenaRef)
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
                    auto varSized = arenaRef.allocateVariableSizedData(fieldSize);
                    nautilus::memcpy(varSized.getContent(), fieldAddress, fieldSize);
                    record.write(fieldName, varSized);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto fieldAddressWithoutOpeningQuote = fieldAddress + nautilus::val<uint32_t>(1);
                    const auto fieldSizeWithoutClosingQuote = fieldSize - nautilus::val<uint32_t>(2);

                    auto varSized = arenaRef.allocateVariableSizedData(fieldSizeWithoutClosingQuote);
                    nautilus::memcpy(varSized.getContent(), fieldAddressWithoutOpeningQuote, fieldSizeWithoutClosingQuote);
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

}
