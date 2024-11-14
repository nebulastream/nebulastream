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

#include <SourceParsers/SourceParserProvider.hpp>
#include <SourceParsers/SourceParserRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::SourceParsers::SourceParserProvider
{

static inline std::string DEFAULT_TUPLE_SEPARATOR = "\n";
static inline std::string DEFAULT_FIELD_DELIMITER = ",";

std::unique_ptr<SourceParser> provideSourceParser(const std::string& parserType, std::shared_ptr<Schema> schema)
{
    if (auto sourceParser
        = SourceParserRegistry::instance().create(parserType, std::move(schema), DEFAULT_TUPLE_SEPARATOR, DEFAULT_FIELD_DELIMITER))
    {
        return std::move(sourceParser.value());
    }
    throw UnknownParserType("unknown type of parser: {}", parserType);
}
}
