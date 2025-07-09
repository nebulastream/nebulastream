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

#include <InputFormatters/InputFormatterProvider.hpp>

#include <memory>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterRegistry.hpp>

namespace NES::InputFormatters::InputFormatterProvider
{

std::unique_ptr<InputFormatterTask> provideInputFormatterTask(const OriginId originId, const Schema& schema, const ParserConfig& config)
{
    if (auto inputFormatter
        = InputFormatterRegistry::instance().create(config.parserType, InputFormatterRegistryArguments{.inputFormatterConfig=config, .numberOfFieldsInSchema=schema.getNumberOfFields()}))
    {
        return std::make_unique<InputFormatterTask>(originId, std::move(inputFormatter.value()), schema, config);
    }
    throw UnknownParserType("unknown type of parser: {}", config.parserType);
}
bool contains(const std::string& parserType)
{
    return InputFormatterRegistry::instance().contains(parserType);
}
}
