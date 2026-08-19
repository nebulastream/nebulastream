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

#include <SequentialUncompiledHL7InputFormatIndexer.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <Hl7Scan.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <UncompiledHl7FIF.hpp>
#include <UncompiledInputFormatIndexerRegistry.hpp>

namespace NES
{

SequentialUncompiledHL7InputFormatIndexer::SequentialUncompiledHL7InputFormatIndexer(const size_t numberOfFieldsInSchema)
    : numberOfFieldsInSchema(numberOfFieldsInSchema), computeBlocks(SimdHl7::selectComputeBlocks())
{
}

void SequentialUncompiledHL7InputFormatIndexer::indexRawBuffer(
    UncompiledHl7FIF& fieldIndexFunction, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledHL7MetaData& metaData) const
{
    const auto messageDelimiter = metaData.getTupleDelimitingBytes();
    fieldIndexFunction.startSetup(numberOfFieldsInSchema, messageDelimiter.size());

    const auto view = rawBuffer.getBufferView();
    const auto [band, pairBandIdx] = fieldIndexFunction.prepareBands(view.size());

    /// 1-byte mode passes msgFirst twice: the flatten ignores msgSecond, the kernel's compare needs A byte.
    const auto flattened = messageDelimiter.size() == 2
        ? SimdHl7::flattenHl7SimdInto<2>(
              band, pairBandIdx, view, metaData.getStructuralChars(), messageDelimiter[0], messageDelimiter[1], computeBlocks)
        : SimdHl7::flattenHl7SimdInto<1>(
              band, pairBandIdx, view, metaData.getStructuralChars(), messageDelimiter[0], messageDelimiter[0], computeBlocks);

    /// No delimiter pair in the buffer: the whole buffer is a spanning-tuple fragment.
    if (flattened.numPairs == 0)
    {
        fieldIndexFunction.markNoTupleDelimiters();
        return;
    }
    /// Complete messages live strictly between consecutive pairs; leading/trailing-fragment events sit in the
    /// band outside [firstPair, lastPair] and are never addressed by the read phase.
    SimdHl7::validateHl7MessageArity(pairBandIdx, flattened.numPairs, numberOfFieldsInSchema);
    fieldIndexFunction.setFlattenResult(pairBandIdx[0], flattened.numPairs - 1);
    fieldIndexFunction.markWithTupleDelimiters(band[pairBandIdx[0]], band[pairBandIdx[flattened.numPairs - 1]]);
}

DescriptorConfig::Config SequentialUncompiledHL7InputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// Same config surface and escape handling as the compiled HL7 indexers.
    for (const auto* const key :
         {"message_delimiter", "segment_delimiter", "field_delimiter", "component_delimiter", "subcomponent_delimiter"})
    {
        if (const auto entry = config.find(key); entry != config.end())
        {
            entry->second = unescapeSpecialCharacters(entry->second);
        }
    }
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledHL7>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterSequentialUncompiledHL7UncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        SequentialUncompiledHL7InputFormatIndexer(arguments.getNumberOfFieldsInSchema()), UncompiledQuotationType::NONE);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialUncompiledHL7InputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialUncompiledHL7InputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialUncompiledHL7InputFormatIndexer&)
{
    return os << fmt::format("SequentialUncompiledHL7InputFormatIndexer()");
}

}
