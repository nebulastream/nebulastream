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

#include <UncompiledInputFormatters/UncompiledInputFormatterProvider.hpp>

#include <memory>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <ErrorHandling.hpp>
#include <UncompiledInputFormatIndexerRegistry.hpp>

namespace NES
{

std::unique_ptr<UncompiledInputFormatterTaskPipeline>
provideUncompiledInputFormatterTask(const Schema& schema, const InputFormatterDescriptor& config)
{
    if (auto inputFormatter
        = UncompiledInputFormatIndexerRegistry::instance().create(config.getInputFormatterType(), UncompiledInputFormatIndexerRegistryArguments(config, schema)))
    {
        return std::move(inputFormatter.value());
    }
    throw UnknownInputFormatterType("unknown type of input formatter: {}", config.getInputFormatterType());
}

bool uncompiledContains(const std::string& parserType)
{
    return UncompiledInputFormatIndexerRegistry::instance().contains(parserType);
}
}
