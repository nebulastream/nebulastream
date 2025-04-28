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

#pragma once

#include <cstddef>
#include <string_view>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <FieldAccessFunction.hpp>

namespace NES::InputFormatters
{

template <bool HasSpanningTuples>
class NativeFormatFieldAccess final : public FieldAccessFunction<NativeFormatFieldAccess<HasSpanningTuples>>
{
public:
    explicit NativeFormatFieldAccess(Memory::AbstractBufferProvider&) { };
    ~NativeFormatFieldAccess() = default;

    void startSetup(const RawTupleBuffer& rawBuffer, const TupleMetaData& tupleMetaData)
    {
        /// Chunk numbers make it impossible to statelessly calculate the offsets of tuples, since we don't know into
        /// how many chunk numbers prior sequence numbers were split.
        /// If we determine the need to support chunk numbers, we should add metadata to buffers, e.g., storing the
        /// offset to the first tuple delimiter. The metadata could be part of the control block, or part of a header
        /// of the data buffer.
        PRECONDITION(
            rawBuffer.getChunkNumber().getRawValue() == ChunkNumber::INITIAL,
            "The NativeFormatFieldAccess does not support chunk numbers yet.");
        this->tupleMetaData = tupleMetaData;
        if constexpr (HasSpanningTuples)
        {
            /// Calculate the overhang of the last tuple of the prior buffer into the current buffer, which is the
            /// offset of the first tuple in the current buffer
            const size_t bytesHeldByPriorBuffers = (rawBuffer.getSequenceNumber().getRawValue() - 1) * rawBuffer.getBufferSize();
            const size_t overhang = (tupleMetaData.sizeOfTupleInBytes - (bytesHeldByPriorBuffers % tupleMetaData.sizeOfTupleInBytes))
                % tupleMetaData.sizeOfTupleInBytes;
            this->offsetOfFirstTuple = overhang;

            this->totalNumberOfTuples = (rawBuffer.getBufferSize() - overhang) / tupleMetaData.sizeOfTupleInBytes;
            this->offsetOfLastTuple = this->offsetOfFirstTuple + (totalNumberOfTuples * tupleMetaData.sizeOfTupleInBytes);
        }
        else
        {
            this->offsetOfFirstTuple = 0;
            this->totalNumberOfTuples = rawBuffer.getBufferSize() / tupleMetaData.sizeOfTupleInBytes;
            this->offsetOfLastTuple = totalNumberOfTuples * tupleMetaData.sizeOfTupleInBytes;
        }
        this->offsetOfCurrentTuple = this->offsetOfFirstTuple;
    }

    [[nodiscard]] FieldOffsetsType applyGetOffsetOfFirstTupleDelimiter() const { return this->offsetOfFirstTuple; }
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfLastTupleDelimiter() const { return this->offsetOfLastTuple; }
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }
    [[nodiscard]] std::string_view
    applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, FieldOffsetsType fieldIdx) const
    {
        PRECONDITION(
            fieldIdx < this->tupleMetaData.fieldSizesInBytes.size(),
            "FieldIdx {} is out of range [0-{}].",
            fieldIdx,
            this->tupleMetaData.fieldSizesInBytes.size());
        const auto sizeOfField = this->tupleMetaData.fieldSizesInBytes[fieldIdx];
        const auto offsetOfCurrentField
            = this->offsetOfFirstTuple + (tupleMetaData.sizeOfTupleInBytes * tupleIdx) + tupleMetaData.fieldOffsetsInBytes[fieldIdx];
        return bufferView.substr(this->offsetOfCurrentTuple + offsetOfCurrentField, sizeOfField);
    }
    void applyAdvanceOneTuple() { this->offsetOfCurrentTuple += tupleMetaData.sizeOfTupleInBytes; }

private:
    TupleMetaData tupleMetaData;
    FieldOffsetsType offsetOfCurrentTuple{};
    FieldOffsetsType offsetOfFirstTuple{};
    FieldOffsetsType offsetOfLastTuple{};
    FieldOffsetsType totalNumberOfTuples{};
};

}
