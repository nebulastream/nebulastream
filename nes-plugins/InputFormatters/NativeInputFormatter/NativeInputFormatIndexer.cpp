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

#include <NativeInputFormatIndexer.hpp>

#include <cstddef>
#include <ostream>

#include <Identifiers/Identifiers.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <NativeFIF.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{

void NativeInputFormatIndexer::indexRawBuffer(
    NativeFIF& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const NativeMetaData& metaData) const
{
    /// If buffer does not contain data, its a spanning tuple
    if (rawBuffer.getRawBuffer().getAvailableMemoryArea().data() == nullptr)
    {
        fieldIndexFunction.setIndexedBuffer(0, static_cast<FieldIndex>(rawBuffer.getBufferView().size()), 1);
        return;
    }
    const auto tupleSize = metaData.getTupleSize();
    const auto bufferActualSize = rawBuffer.getBufferView().size();

    /// The InputFormatter wraps a constructed spanning tuple in a RawTupleBuffer that has no underlying TupleBuffer
    /// (its sequence number is therefore INVALID). In that case the buffer contains exactly one complete tuple at
    /// offset 0, and we can shortcut the alignment computation.
    if (rawBuffer.getSequenceNumber() == INVALID<SequenceNumber>)
    {
        INVARIANT(
            bufferActualSize == tupleSize,
            "Spanning native tuple buffer must have size equal to tupleSize ({}), got {}",
            tupleSize,
            bufferActualSize);
        fieldIndexFunction.setIndexedBuffer(0, static_cast<FieldIndex>(bufferActualSize), 1);
        return;
    }

    /// Buffer alignment: native sources do not embed delimiters, so the position of the first/last full tuple within
    /// this buffer depends solely on the running global byte offset. Buffers are assumed to be always full (size ==
    /// metaData.getBufferSize()), so the global offset is `(sequenceNumber - 1) * fullBufferSize`.
    const auto sequenceNumberValue = rawBuffer.getSequenceNumber().getRawValue();
    const auto fullBufferSize = metaData.getBufferSize();
    const auto bytesBefore = (sequenceNumberValue - 1) * fullBufferSize;

    const auto leadingPartialBytes = (tupleSize - (bytesBefore % tupleSize)) % tupleSize;
    const auto trailingPartialBytes = (bytesBefore + bufferActualSize) % tupleSize;

    INVARIANT(
        leadingPartialBytes + trailingPartialBytes <= bufferActualSize,
        "Tuple is larger than a single buffer (leading partial {} + trailing partial {} > buffer size {})",
        leadingPartialBytes,
        trailingPartialBytes,
        bufferActualSize);

    const auto offsetOfFirstTuple = leadingPartialBytes;
    const auto offsetOfLastTuple = bufferActualSize - trailingPartialBytes;
    const auto numberOfCompleteTuples = (offsetOfLastTuple - offsetOfFirstTuple) / tupleSize;

    fieldIndexFunction.setIndexedBuffer(
        static_cast<FieldIndex>(offsetOfFirstTuple), static_cast<FieldIndex>(offsetOfLastTuple), numberOfCompleteTuples);
}

std::ostream& operator<<(std::ostream& os, const NativeInputFormatIndexer&)
{
    return os << fmt::format("NativeInputFormatIndexer()");
}

DescriptorConfig::Config NativeInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersNative>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType
RegisterSystemNativeInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(NativeInputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterSystemNativeInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return NativeInputFormatIndexer::validateAndFormat(arguments.config);
}

}
