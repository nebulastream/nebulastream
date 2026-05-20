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

#include <InputParserUtil.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
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
#include <InputParser.hpp>
#include <InputParserRegistry.hpp>
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

void parseRawValueIntoRecord(
    const DataType dataType,
    const std::string& parserType,
    Record& record,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& fieldName,
    const std::vector<std::string>& nullValues)
{
    if (dataType.type == DataType::Type::UNDEFINED)
    {
        throw NotImplemented("Cannot parse undefined type.");
    }
    /// Get parser of the datatype from the registry and use it to parse the value
    const std::unique_ptr<InputParser> parser = provideInputParser(parserType);
    const VarVal parsedVal = parser->parseToVarVal(dataType.nullable, fieldAddress, fieldSize, nullValues);
    record.write(fieldName, parsedVal);
}

std::unique_ptr<InputParser> provideInputParser(const std::string& parserType)
{
    constexpr InputParserRegistryArguments arguments{};
    if (auto parser = InputParserRegistry::instance().create(parserType, arguments))
    {
        return std::move(parser.value());
    }
    throw UnknownInputParserType("Unknown Input Parser: {}", parserType);
}
}
