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

#include <OutputFormatters/OutputFormatterUtil.hpp>

#include <memory>
#include <utility>
#include <OutputFormatters/OutputParser.hpp>
#include <ErrorHandling.hpp>
#include <OutputParserRegistry.hpp>

namespace NES
{
std::unique_ptr<OutputParser> provideOutputParser(const std::string& parserType)
{
    constexpr OutputParserRegistryArguments arguments{};
    if (auto parser = OutputParserRegistry::instance().create(parserType, arguments))
    {
        return std::move(parser.value());
    }
    throw UnknownOutputParserType("Unknown Output Parser: {}", parserType);
}
}
