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

#include <SIMDJSONInputFormatIndexer.hpp>

#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <fmt/format.h>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDJSONRawBufferIndex.hpp>
#include <simdjson.h>

namespace NES
{

std::size_t SIMDJSONInputFormatIndexer::requiredTailPadding() const noexcept
{
    return simdjson::SIMDJSON_PADDING;
}

std::unique_ptr<RawBufferIndex> SIMDJSONInputFormatIndexer::indexRawBuffer(const std::string_view rawBuffer) const
{
    auto rawBufferIndex = std::make_unique<SIMDJSONRawBufferIndex>();

    const auto offsetOfFirstTuple = static_cast<FieldIndex>(rawBuffer.find(TUPLE_DELIMITER));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTuple' to a value larger than the buffer size to tell
    /// the InputFormatter that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTuple == static_cast<FieldIndex>(std::string::npos))
    {
        rawBufferIndex->markNoTupleDelimiters();
        return rawBufferIndex;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    const auto startIdxOfNextTuple = offsetOfFirstTuple + DELIMITER_SIZE;

    const auto jsonSV = rawBuffer.substr(startIdxOfNextTuple);
    const auto [isNoTuple, truncatedBytes] = rawBufferIndex->indexJSON(jsonSV);
    const auto offsetOfLastTuple = static_cast<FieldIndex>(
        (isNoTuple) ? offsetOfFirstTuple : rawBuffer.size() - truncatedBytes - this->getTupleDelimitingBytes().size());

    rawBufferIndex->markWithTupleDelimiters(offsetOfFirstTuple, offsetOfLastTuple);
    return rawBufferIndex;
}

std::ostream& SIMDJSONInputFormatIndexer::toString(std::ostream& str) const
{
    return str << fmt::format("SIMDJSONInputFormatIndexer(tupleDelimiter: {})", SIMDJSONInputFormatIndexer::TUPLE_DELIMITER);
}

DescriptorConfig::Config SIMDJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSIMDJSON>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType RegisterJSONInputFormatIndexer(InputFormatIndexerRegistryArguments arguments)
{
    return arguments.createInputFormatterWithIndexer(
        SIMDJSONInputFormatIndexer::create(arguments.getInputFormatterConfig(), arguments.getInputMemoryProvider()));
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterJSONInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return SIMDJSONInputFormatIndexer::validateAndFormat(arguments.config);
}
}
