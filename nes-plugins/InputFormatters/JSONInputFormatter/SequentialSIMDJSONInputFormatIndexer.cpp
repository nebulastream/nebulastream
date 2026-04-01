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

#include <SequentialSIMDJSONInputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <SIMDJSONFIF.hpp>

#include "InputFormatterValidationRegistry.hpp"

namespace NES
{

void SequentialSIMDJSONInputFormatIndexer::indexRawBuffer(
    SIMDJSONFIF& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const SIMDJSONMetaData& metaData)
{
    /// Sequential mode: buffer is aligned, pass the ENTIRE buffer to simdjson (no skipping to after first delimiter)
    const auto jsonSV = rawBuffer.getBufferView();
    const auto [isNoTuple, truncatedBytes] = fieldIndexFunction.indexJSON(jsonSV);

    if (isNoTuple)
    {
        fieldIndexFunction.markNoTupleDelimiters();
        return;
    }

    const auto offsetOfFirstTuple = static_cast<FieldIndex>(rawBuffer.getBufferView().find(TUPLE_DELIMITER));
    const auto offsetOfLastTuple = rawBuffer.getBufferView().size() - truncatedBytes - metaData.getTupleDelimitingBytes().size();

    fieldIndexFunction.markWithTupleDelimiters(offsetOfFirstTuple, offsetOfLastTuple);
}

std::ostream& operator<<(std::ostream& os, const SequentialSIMDJSONInputFormatIndexer&)
{
    return os << fmt::format(
               "SequentialSIMDJSONInputFormatIndexer(tupleDelimiter: {})",
               SequentialSIMDJSONInputFormatIndexer::TUPLE_DELIMITER);
}

DescriptorConfig::Config SequentialSIMDJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSIMDJSON>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType RegisterSequentialJSONInputFormatIndexer(InputFormatIndexerRegistryArguments arguments)
{
    return arguments.createInputFormatterWithIndexer(SequentialSIMDJSONInputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialJSONInputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return SequentialSIMDJSONInputFormatIndexer::validateAndFormat(arguments.config);
}

}
