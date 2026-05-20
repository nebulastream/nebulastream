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
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <InputParser.hpp>
#include <InputParserRegistry.hpp>

namespace NES
{
std::unique_ptr<InputParser> provideInputParser(const std::string& parserType, const InputParserConfig& config)
{
    /// Resolve the "Nullable" member
    const std::string completeParserName = config.nullable ? "Nullable" + parserType : parserType;
    const InputParserRegistryArguments arguments{.quotedText = config.quotedText, .hasTrailingSpaces = config.hasTrailingSpace};
    if (auto parser = InputParserRegistry::instance().create(completeParserName, arguments))
    {
        return std::move(parser.value());
    }
    throw UnknownInputParserType("Unknown Input Parser: {}", parserType);
}
}
