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

#include <SchemaJSONInputFormatIndexer.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{

void SchemaJSONInputFormatIndexer::indexRawBuffer(
    SchemaJSONFieldIndex& fieldIndex, const RawTupleBuffer& rawBuffer, const SchemaJSONMetaData& metaData) const
{
    const auto numFields = static_cast<uint32_t>(metaData.getNumberOfFields());
    fieldIndex.startSetup(numFields);

    const auto bufferView = rawBuffer.getBufferView();
    const auto offsetOfFirstTupleDelimiter = static_cast<FieldIndex>(bufferView.find(TUPLE_DELIMITER));

    /// No tuple delimiter in the buffer: signal the InputFormatter that the whole buffer is a spanning-tuple
    /// fragment (handled by the SequenceShredder) and return.
    if (offsetOfFirstTupleDelimiter == static_cast<FieldIndex>(std::string_view::npos))
    {
        fieldIndex.markNoTupleDelimiters();
        return;
    }

    /// Records that both start AND end inside this buffer live strictly between the first and the last tuple
    /// delimiter -- exactly the CSV indexer's contract. The leading fragment (before the first delimiter) and
    /// the trailing fragment (after the last) are resolved as spanning tuples by the InputFormatter.
    const auto offsetOfLastTupleDelimiter = static_cast<FieldIndex>(bufferView.rfind(TUPLE_DELIMITER));
    if (offsetOfLastTupleDelimiter > offsetOfFirstTupleDelimiter)
    {
        const auto base = static_cast<FieldIndex>(offsetOfFirstTupleDelimiter + 1);
        const auto region = bufferView.substr(base, static_cast<size_t>(offsetOfLastTupleDelimiter) - base);
        schemaJsonIndexIntoFieldIndex(fieldIndex, region, base, numFields);
    }

    fieldIndex.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

DescriptorConfig::Config SchemaJSONInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSchemaJSON>(std::move(config), NAME);
}

std::ostream& operator<<(std::ostream& os, const SchemaJSONInputFormatIndexer&)
{
    return os << fmt::format("SchemaJSONInputFormatIndexer(tupleDelimiter: {})", SchemaJSONInputFormatIndexer::TUPLE_DELIMITER);
}

InputFormatIndexerRegistryReturnType
RegisterSchemaJSONInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SchemaJSONInputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSchemaJSONInputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return SchemaJSONInputFormatIndexer::validateAndFormat(arguments.config);
}

}
