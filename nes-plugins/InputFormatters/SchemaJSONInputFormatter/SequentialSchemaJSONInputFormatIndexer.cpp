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

#include <SequentialSchemaJSONInputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <SchemaJSONInputFormatIndexer.hpp>

namespace NES
{

DescriptorConfig::Config SequentialSchemaJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSchemaJSON>(std::move(config), NAME);
}

std::ostream& operator<<(std::ostream& os, const SequentialSchemaJSONInputFormatIndexer&)
{
    return os << fmt::format(
               "SequentialSchemaJSONInputFormatIndexer(tupleDelimiter: {})", SequentialSchemaJSONInputFormatIndexer::TUPLE_DELIMITER);
}

InputFormatIndexerRegistryReturnType
RegisterSequentialSchemaJSONInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SequentialSchemaJSONInputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterSequentialSchemaJSONInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialSchemaJSONInputFormatIndexer::validateAndFormat(arguments.config);
}

}
