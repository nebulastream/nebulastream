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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <CSVInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatterRegistry.hpp>

namespace NES::InputFormatters
{

CSVInputFormatter::CSVInputFormatter(Sources::ParserConfig parserConfig) : config(std::move(parserConfig))
{
}

void CSVInputFormatter::indexTuple(
    const std::string_view tuple,
    FieldOffsetsType* fieldOffsets,
    const FieldOffsetsType startIdxOfCurrentTuple,
    const FieldOffsetsType endIdxOfCurrentTuple) const
{
    PRECONDITION(fieldOffsets != nullptr, "FieldOffsets cannot be null.");
    PRECONDITION(startIdxOfCurrentTuple <= endIdxOfCurrentTuple, "The start index of a tuple cannot be larger than the end index.");
    /// Iterate over all fields, parse the string values and store the field offsets.
    size_t currentFieldOffset = 0;
    bool hasAnotherField = true;
    while (hasAnotherField)
    {
        const auto fieldOffset = startIdxOfCurrentTuple + currentFieldOffset;
        *fieldOffsets = fieldOffset;
        ++fieldOffsets;
        currentFieldOffset = tuple.find(this->config.fieldDelimiter, currentFieldOffset + this->config.fieldDelimiter.size());
        hasAnotherField = currentFieldOffset != (std::string::npos);
        currentFieldOffset += this->config.fieldDelimiter.size();
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    *fieldOffsets = endIdxOfCurrentTuple;
}

InputFormatter::BufferOffsets CSVInputFormatter::indexBuffer(std::string_view bufferView, FieldOffsets& fieldOffsets) const
{
    // Todo: add PRECONDITION/INVARIANT on the caller side (for bufferView!)
    const auto sizeOfTupleDelimiter = this->config.tupleDelimiter.size();

    const auto offsetOfFirstTupleDelimiter = static_cast<FieldOffsetsType>(bufferView.find(this->config.tupleDelimiter));
    size_t startIdxOfCurrentTuple = offsetOfFirstTupleDelimiter + sizeOfTupleDelimiter;
    size_t endIdxOfCurrentTuple = bufferView.find(this->config.tupleDelimiter, startIdxOfCurrentTuple);

    while (endIdxOfCurrentTuple != std::string::npos)
    {
        auto* tupleOffsetPtr = fieldOffsets.writeNextTuple();
        const auto sizeOfCurrentTuple = endIdxOfCurrentTuple - startIdxOfCurrentTuple;
        /// WE ALWAYS skip the first partial tuple and start with the first full tuple delimiter, thus, we can ALWAYS add the size of the tuple delimiter
        const auto currentTuple = std::string_view(bufferView.begin() + startIdxOfCurrentTuple, sizeOfCurrentTuple);

        indexTuple(currentTuple, tupleOffsetPtr, startIdxOfCurrentTuple, endIdxOfCurrentTuple);

        startIdxOfCurrentTuple = endIdxOfCurrentTuple + sizeOfTupleDelimiter;
        endIdxOfCurrentTuple = bufferView.find(this->config.tupleDelimiter, startIdxOfCurrentTuple);
    }
    const auto offsetOfLastTupleDelimiter = static_cast<FieldOffsetsType>(startIdxOfCurrentTuple - sizeOfTupleDelimiter);
    return {offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter};
}

std::ostream& CSVInputFormatter::toString(std::ostream& os) const
{
    os << "CSVInputFormatter: {}\n";
    return os;
}

InputFormatterRegistryReturnType InputFormatterGeneratedRegistrar::RegisterCSVInputFormatter(InputFormatterRegistryArguments arguments)
{
    return std::make_unique<CSVInputFormatter>(std::move(arguments.inputFormatterConfig));
}

}
