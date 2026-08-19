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

#include <SequentialUncompiledSchemaJSONInputFormatIndexer.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <fmt/format.h>
#include <InputFormatterValidationRegistry.hpp>
#include <UncompiledInputFormatIndexerRegistry.hpp>
#include <UncompiledSchemaJSONFIF.hpp>

namespace NES
{

SequentialUncompiledSchemaJSONInputFormatIndexer::SequentialUncompiledSchemaJSONInputFormatIndexer(const size_t numberOfFieldsInSchema)
    : numberOfFieldsInSchema(numberOfFieldsInSchema)
{
}

void SequentialUncompiledSchemaJSONInputFormatIndexer::indexRawBuffer(
    UncompiledSchemaJSONFIF& fieldIndexFunction,
    const UncompiledRawTupleBuffer& rawBuffer,
    const UncompiledSchemaJSONMetaData& metaData) const
{
    fieldIndexFunction.startSetup(static_cast<uint32_t>(numberOfFieldsInSchema));

    const auto view = rawBuffer.getBufferView();
    const char tupleDelimiter = metaData.getTupleDelimitingBytes().front();

    const auto offsetOfFirstTupleDelimiter = view.find(tupleDelimiter);
    if (offsetOfFirstTupleDelimiter == std::string_view::npos)
    {
        fieldIndexFunction.markNoTupleDelimiters();
        return;
    }

    /// Records that both start AND end inside this view live strictly between the first and the last tuple
    /// delimiter -- the CSV band contract. The leading fragment is completed by the sequential task's carry
    /// accumulator (as a delimiter-wrapped spanning view routed back through this function), the trailing
    /// fragment is carried to the next buffer.
    const auto offsetOfLastTupleDelimiter = view.rfind(tupleDelimiter);
    if (offsetOfLastTupleDelimiter > offsetOfFirstTupleDelimiter)
    {
        const auto base = offsetOfFirstTupleDelimiter + 1;
        const auto region = view.substr(base, offsetOfLastTupleDelimiter - base);
        /// The raw pool buffer allows stage 1's 64B over-read; an assembled spanning-tuple view (a bare
        /// std::string set via setSpanningTuple) does not and needs the padded copy inside indexRegion.
        const bool viewIsRawPoolBuffer = view.data() == rawBuffer.getRawBuffer().getAvailableMemoryArea<char>().data();
        fieldIndexFunction.indexRegion(region, static_cast<UncompiledFieldIndex>(base), viewIsRawPoolBuffer);
    }

    fieldIndexFunction.markWithTupleDelimiters(
        static_cast<UncompiledFieldIndex>(offsetOfFirstTupleDelimiter), static_cast<UncompiledFieldIndex>(offsetOfLastTupleDelimiter));
}

DescriptorConfig::Config
SequentialUncompiledSchemaJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledSchemaJSON>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterSequentialUncompiledSchemaJSONUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    /// JSON string values are double-quoted; the VARSIZED parse function strips the surrounding quotes per
    /// field (same contract as the compiled SchemaJSON's QuotationType::DOUBLE_QUOTE -- no unescaping).
    return arguments.createUncompiledInputFormatterTaskPipeline(
        SequentialUncompiledSchemaJSONInputFormatIndexer(arguments.getNumberOfFieldsInSchema()), UncompiledQuotationType::DOUBLE_QUOTE);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialUncompiledSchemaJSONInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialUncompiledSchemaJSONInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialUncompiledSchemaJSONInputFormatIndexer&)
{
    return os << fmt::format("SequentialUncompiledSchemaJSONInputFormatIndexer()");
}

}
