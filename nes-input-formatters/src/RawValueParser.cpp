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
#include <val_arith.hpp>
#include <val_bool.hpp>
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
    /// Raw values will be transformed into a lazy representation of pointer, size and underlying datatype instead of being parsed immediatly.
    /// Should a field be needed in its parsed form for the execution of a logical function, it will be parsed in the overridden function in LazyValueRepresentation
    if (physicalType == DataType::Type::UNDEFINED)
    {
        throw NotImplemented("Cannot parse undefined type.");
    }
    if (physicalType == DataType::Type::CHAR && quotationType == QuotationType::DOUBLE_QUOTE)
    {
        const LazyValueRepresentation lazyVal(
            fieldAddress + nautilus::val<uint32_t>(1), fieldSize - nautilus::val<uint32_t>(2), physicalType);
        record.write(fieldName, lazyVal);
        return;
    }
    const LazyValueRepresentation lazyVal(fieldAddress, fieldSize, physicalType);
    record.write(fieldName, lazyVal);
}

}
