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

#include <OutputFormatters/FloatOutputParserProvider.hpp>

#include <memory>
#include <string>
#include <utility>
#include <OutputFormatters/FloatOutputParser.hpp>
#include <FloatOutputParserRegistry.hpp>
#include "Util/Logger/Logger.hpp"

namespace NES::FloatOutputParserProvider
{
std::unique_ptr<FloatOutputParser> provideFloatOutputParser(const std::string& parserType)
{
    auto args = FloatOutputParserRegistryArguments{};
    if (auto floatOutputParser = FloatOutputParserRegistry::instance().create(parserType, args))
    {
        return std::move(floatOutputParser.value());
    }
    /// We will automatically use our default parsers, if the parserType is unknown
    NES_WARNING("Unknown float output parser type: {}\nUsing the default parser instead.", parserType);
    return std::move(FloatOutputParserRegistry::instance().create("Default", args).value());
}
}
