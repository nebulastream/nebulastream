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

#include <SequentialUncompiledSIMDJSONInputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <UncompiledInputFormatIndexerRegistry.hpp>
#include <UncompiledSIMDJSONFIF.hpp>

#include <InputFormatterValidationRegistry.hpp>

namespace NES
{

void SequentialUncompiledSIMDJSONInputFormatIndexer::indexRawBuffer(
    UncompiledSIMDJSONFIF& fieldIndexFunction, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledSIMDJSONMetaData& metaData)
{
    /// Sequential mode: buffer is aligned, pass the ENTIRE buffer to simdjson (no skipping to after first delimiter)
    const auto jsonSV = rawBuffer.getBufferView();
    const auto [isNoTuple, truncatedBytes] = fieldIndexFunction.indexJSON(jsonSV, metaData);

    if (isNoTuple)
    {
        fieldIndexFunction.markNoTupleDelimiters();
        return;
    }

    /// Find the first delimiter position (needed by the task for spanning tuple assembly)
    const auto offsetOfFirstTuple = static_cast<UncompiledFieldIndex>(rawBuffer.getBufferView().find(TUPLE_DELIMITER));
    const auto offsetOfLastTuple = rawBuffer.getBufferView().size() - truncatedBytes - metaData.getTupleDelimitingBytes().size();

    fieldIndexFunction.markWithTupleDelimiters(offsetOfFirstTuple, offsetOfLastTuple);
}

std::ostream& operator<<(std::ostream& os, const SequentialUncompiledSIMDJSONInputFormatIndexer&)
{
    return os << fmt::format(
               "SequentialUncompiledSIMDJSONInputFormatIndexer(tupleDelimiter: {})",
               SequentialUncompiledSIMDJSONInputFormatIndexer::TUPLE_DELIMITER);
}

DescriptorConfig::Config
SequentialUncompiledSIMDJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledSIMDJSON>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterSequentialUncompiledJSONUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        SequentialUncompiledSIMDJSONInputFormatIndexer{}, UncompiledQuotationType::DOUBLE_QUOTE);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialUncompiledJSONInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialUncompiledSIMDJSONInputFormatIndexer::validateAndFormat(arguments.config);
}

}
