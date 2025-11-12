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

#include <ostream>
#include <string>
#include <string_view>

#include <InputFormatters/InputFormatterTupleBufferRef.hpp>
#include <fmt/format.h>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatter.hpp>
#include <SIMDJSONFIF.hpp>

namespace NES
{

void SIMDJSONInputFormatIndexer::indexRawBuffer(
    SIMDJSONFIF& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const SIMDJSONMetaData& metaData)
{
    const auto offsetOfFirstTuple = static_cast<FieldIndex>(rawBuffer.getBufferView().find(TUPLE_DELIMITER));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTuple' to a value larger than the buffer size to tell
    /// the InputFormatter that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTuple == static_cast<FieldIndex>(std::string::npos))
    {
        fieldIndexFunction.markNoTupleDelimiters();
        return;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    const auto startIdxOfNextTuple = offsetOfFirstTuple + DELIMITER_SIZE;

    const auto jsonSV = rawBuffer.getBufferView().substr(startIdxOfNextTuple);
    const auto [isNoTuple, truncatedBytes] = fieldIndexFunction.indexJSON(jsonSV);
    const auto offsetOfLastTuple
        = (isNoTuple) ? offsetOfFirstTuple : rawBuffer.getBufferView().size() - truncatedBytes - metaData.getTupleDelimitingBytes().size();

    fieldIndexFunction.markWithTupleDelimiters(offsetOfFirstTuple, offsetOfLastTuple);
}

std::ostream& operator<<(std::ostream& os, const SIMDJSONInputFormatIndexer&)
{
    return os << fmt::format(
               "SIMDJSONInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {})",
               SIMDJSONInputFormatIndexer::TUPLE_DELIMITER,
               SIMDJSONInputFormatIndexer::FIELD_DELIMITER);
}

InputFormatIndexerRegistryReturnType RegisterJSONInputFormatIndexer(InputFormatIndexerRegistryArguments arguments)
{
    return arguments.createInputFormatterWithIndexer(SIMDJSONInputFormatIndexer{});
}

}
