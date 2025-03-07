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
#include <Util/Logger/Logger.hpp>
#include <AsyncInputFormatterRegistry.hpp>
#include <CSVInputFormatter.hpp>
#include <FieldOffsetsIterator.hpp>
#include <SyncInputFormatterRegistry.hpp>

namespace NES::InputFormatters
{

void CSVInputFormatter::indexSpanningTuple(
    const std::string_view tuple,
    const std::string_view fieldDelimiter,
    FieldOffsetsType* fieldOffsets,
    const FieldOffsetsType startIdxOfCurrentTuple,
    const FieldOffsetsType endIdxOfCurrentTuple,
    FieldOffsetsType currentFieldIndex)
{
    /// Iterate over all fields, parse the string values and write the formatted data into the TBF.
    size_t currentFieldOffset = 0;
    bool hasAnotherField = true;
    while (hasAnotherField)
    {
        const auto fieldOffset = startIdxOfCurrentTuple + currentFieldOffset;
        fieldOffsets[currentFieldIndex] = fieldOffset;
        ++currentFieldIndex;
        currentFieldOffset = tuple.find(fieldDelimiter, currentFieldOffset + fieldDelimiter.size()) + fieldDelimiter.size();
        hasAnotherField = currentFieldOffset != (std::string::npos + fieldDelimiter.size());
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsets[currentFieldIndex] = endIdxOfCurrentTuple;
    ++currentFieldIndex;
}

void CSVInputFormatter::indexRawBuffer(
    const char* rawTB,
    const uint32_t numberOfBytesInRawTB,
    FieldOffsetIterator& fieldOffsets,
    const std::string_view tupleDelimiter,
    const std::string_view fieldDelimiter,
    FieldOffsetsType& offsetOfFirstTupleDelimiter,
    FieldOffsetsType& offsetOfLastTupleDelimiter)
{
    const auto sizeOfTupleDelimiter = tupleDelimiter.size();
    const auto bufferView = std::string_view(rawTB, numberOfBytesInRawTB);

    offsetOfFirstTupleDelimiter = static_cast<FieldOffsetsType>(bufferView.find(tupleDelimiter));
    size_t startIdxOfCurrentTuple = offsetOfFirstTupleDelimiter + sizeOfTupleDelimiter;
    size_t endIdxOfCurrentTuple = bufferView.find(tupleDelimiter, startIdxOfCurrentTuple);

    FieldOffsetsType tuplesInCurrentBuffer = 0;
    while (endIdxOfCurrentTuple != std::string::npos)
    {
        const auto sizeOfCurrentTuple = endIdxOfCurrentTuple - startIdxOfCurrentTuple;
        /// WE ALWAYS skip the first partial tuple and start with the first full tuple delimiter, thus, we can ALWAYS add the size of the tuple delimiter
        const auto currentTuple = std::string_view(bufferView.begin() + startIdxOfCurrentTuple, sizeOfCurrentTuple);

        /// Iterate over all fields, parse the string values and write the formatted data into the TBF.
        size_t currentFieldOffset = 0;
        bool hasAnotherField = true;
        while (hasAnotherField)
        {
            const auto fieldOffset = startIdxOfCurrentTuple + currentFieldOffset;
            fieldOffsets.write(fieldOffset);
            ++fieldOffsets;
            currentFieldOffset = currentTuple.find(fieldDelimiter, currentFieldOffset + fieldDelimiter.size()) + fieldDelimiter.size();
            hasAnotherField = currentFieldOffset != (std::string::npos + fieldDelimiter.size());
        }
        /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
        fieldOffsets.write(endIdxOfCurrentTuple);
        ++fieldOffsets;

        startIdxOfCurrentTuple = endIdxOfCurrentTuple + sizeOfTupleDelimiter;
        endIdxOfCurrentTuple = bufferView.find(tupleDelimiter, startIdxOfCurrentTuple);
        ++tuplesInCurrentBuffer;
    }
    offsetOfLastTupleDelimiter = static_cast<FieldOffsetsType>(startIdxOfCurrentTuple - sizeOfTupleDelimiter);
}

std::ostream& CSVInputFormatter::toString(std::ostream& os) const
{
    os << "CSVInputFormatter: {}\n";
    return os;
}

std::unique_ptr<AsyncInputFormatterRegistryReturnType>
AsyncInputFormatterGeneratedRegistrar::RegisterCSVAsyncInputFormatter(AsyncInputFormatterRegistryArguments)
{
    return std::make_unique<CSVInputFormatter>();
}

std::unique_ptr<SyncInputFormatterRegistryReturnType>
SyncInputFormatterGeneratedRegistrar::RegisterCSVSyncInputFormatter(SyncInputFormatterRegistryArguments)
{
    return std::make_unique<CSVInputFormatter>();
}

}
