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

#include <UncompiledSIMDJSONInputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <UncompiledInputFormatIndexerRegistry.hpp>
#include <UncompiledSIMDJSONFIF.hpp>

#include <InputFormatterValidationRegistry.hpp>

namespace NES
{

void UncompiledSIMDJSONInputFormatIndexer::indexRawBuffer(
    UncompiledSIMDJSONFIF& fieldIndexFunction, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledSIMDJSONMetaData& metaData)
{
    const auto offsetOfFirstTuple = static_cast<UncompiledFieldIndex>(rawBuffer.getBufferView().find(TUPLE_DELIMITER));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTuple' to a value larger than the buffer size to tell
    /// the InputFormatter that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTuple == static_cast<UncompiledFieldIndex>(std::string::npos))
    {
        fieldIndexFunction.markNoTupleDelimiters();
        return;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    const auto startIdxOfNextTuple = offsetOfFirstTuple + DELIMITER_SIZE;

    const auto jsonSV = rawBuffer.getBufferView().substr(startIdxOfNextTuple);
    const auto [isNoTuple, truncatedBytes] = fieldIndexFunction.indexJSON(jsonSV, metaData);
    const auto offsetOfLastTuple
        = (isNoTuple) ? offsetOfFirstTuple : rawBuffer.getBufferView().size() - truncatedBytes - metaData.getTupleDelimitingBytes().size();

    fieldIndexFunction.markWithTupleDelimiters(offsetOfFirstTuple, offsetOfLastTuple);
}

std::ostream& operator<<(std::ostream& os, const UncompiledSIMDJSONInputFormatIndexer&)
{
    return os << fmt::format(
               "UncompiledSIMDJSONInputFormatIndexer(tupleDelimiter: {})", UncompiledSIMDJSONInputFormatIndexer::TUPLE_DELIMITER);
}

DescriptorConfig::Config UncompiledSIMDJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledSIMDJSON>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterUncompiledJSONUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        UncompiledSIMDJSONInputFormatIndexer{}, UncompiledQuotationType::DOUBLE_QUOTE);
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterUncompiledJSONInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return UncompiledSIMDJSONInputFormatIndexer::validateAndFormat(arguments.config);
}

}
