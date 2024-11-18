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

#include <memory>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <SourceParsers/SourceParserProvider.hpp>
#include <ErrorHandling.hpp>
#include <SourceParserRegistry.hpp>

namespace NES::SourceParsers::SourceParserProvider
{

std::unique_ptr<SourceParser>
provideSourceParser(const std::string& parserType, const Schema& schema, std::string tupleDelimiter, std::string fieldDelimiter)
{
    if (auto sourceParser
        = SourceParserRegistry::instance().create(parserType, schema, std::move(tupleDelimiter), std::move(fieldDelimiter)))
    {
        return std::move(sourceParser.value());
    }
    throw UnknownParserType("unknown type of parser: {}", parserType);
}
}
