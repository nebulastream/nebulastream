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
#include <cstdint>
#include <string_view>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
using FieldIndex = uint32_t;
using SequenceNumberType = SequenceNumber::Underlying;

/// Takes a tuple buffer containing raw, unformatted data and wraps it into an object that fulfills the following purposes:
/// 1. The RawTupleBuffer allows its users to operate on string_views, instead of handling raw pointers (which a TupleBuffer would require)
/// 2. It exposes only those functions of the TupleBuffer that are required for formatting
/// 3. It selectively exposes the reduced set of functions, to prohibit setting, e.g., the SequenceNumber in InputFormatIndexer
/// 4. It exposes functions with more descriptive names, e.g., `getNumberOfBytes()` instead of `getNumberOfTuples`
/// 5. The type (RawTupleBuffer) makes it clear that we are dealing with raw data and not with (formatted) tuples

template <InputFormatIndexerType FormatterType>
class InputFormatter;

class RawTupleBuffer
{
    template <InputFormatIndexerType FormatterType>
    friend class InputFormatter;

public:
    RawTupleBuffer() = default;
    ~RawTupleBuffer() = default;
    explicit RawTupleBuffer(TupleBuffer rawTupleBuffer)
        : rawBuffer(std::move(rawTupleBuffer))
        , bufferView(rawBuffer.getAvailableMemoryArea<char>().data(), rawBuffer.getNumberOfTuples()) { };


    RawTupleBuffer(RawTupleBuffer&& other) noexcept = default;
    RawTupleBuffer& operator=(RawTupleBuffer&& other) noexcept = default;
    RawTupleBuffer(const RawTupleBuffer& other) = default;
    RawTupleBuffer& operator=(const RawTupleBuffer& other) = default;

    [[nodiscard]] size_t getNumberOfBytes() const noexcept { return rawBuffer.getNumberOfTuples(); }

    [[nodiscard]] size_t getBufferSize() const noexcept { return rawBuffer.getBufferSize(); }

    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept { return rawBuffer.getSequenceNumber(); }

    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept { return rawBuffer.getChunkNumber(); }

    [[nodiscard]] OriginId getOriginId() const noexcept { return rawBuffer.getOriginId(); }

    [[nodiscard]] std::string_view getBufferView() const noexcept { return bufferView; }

    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept { return rawBuffer.getNumberOfTuples(); }

    void setNumberOfTuples(const uint64_t numberOfTuples) const noexcept { rawBuffer.setNumberOfTuples(numberOfTuples); }

    /// Allows to emit the underlying buffer without exposing it to the outside
    void emit(PipelineExecutionContext& pec, const PipelineExecutionContext::ContinuationPolicy continuationPolicy) const
    {
        pec.emitBuffer(rawBuffer, continuationPolicy);
    }

    void repeat(PipelineExecutionContext& pec) const { pec.repeatTask(rawBuffer, std::chrono::milliseconds{0}); }

    [[nodiscard]] const TupleBuffer& getRawBuffer() const noexcept { return rawBuffer; }

    void setSpanningTuple(const std::string_view spanningTuple) { this->bufferView = spanningTuple; }

private:
    TupleBuffer rawBuffer;
    std::string_view bufferView;

    /// Used by InputFormatter to pass an arena-allocated spanning tuple to an InputFormatIndexer
    explicit RawTupleBuffer(const char* rawDataPtr, const size_t sizeOfSpanningTuple) : bufferView(rawDataPtr, sizeOfSpanningTuple) { };
};

/// A staged buffer represents a raw buffer that the input formatter cannot process independently, because it contains spanning tuples.
/// The input formatter keeps the buffer staged, together with the locations of the first and last tuple delimiter, until it can determine
/// all spanning tuple(s) starting/ending in or containing the StagedBuffer.
struct StagedBuffer
{
private:
    friend class SequenceShredder;

public:
    StagedBuffer() = default;

    StagedBuffer(RawTupleBuffer rawTupleBuffer, const uint32_t offsetOfFirstTupleDelimiter, const uint32_t offsetOfLastTupleDelimiter)
        : rawBuffer(std::move(rawTupleBuffer))
        , sizeOfBufferInBytes(this->rawBuffer.getNumberOfBytes())
        , offsetOfFirstTupleDelimiter(offsetOfFirstTupleDelimiter)
        , offsetOfLastTupleDelimiter(offsetOfLastTupleDelimiter)
    {
    }

    [[nodiscard]] std::string_view getBufferView() const { return rawBuffer.getBufferView(); }

    /// Returns the _first_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of a spanning tuple that _ends_ in the staged buffer.
    [[nodiscard]] std::string_view getLeadingBytes() const { return rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter); }

    /// Returns the _last_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of spanning tuple that _starts_ in the staged buffer.
    [[nodiscard]] std::string_view getTrailingBytes(const size_t sizeOfTupleDelimiter) const
    {
        INVARIANT(
            sizeOfBufferInBytes >= offsetOfLastTupleDelimiter + sizeOfTupleDelimiter,
            "Invalid trailing bytes. Size of buffer: {} < {} (offsetOfLastTupleDelimiter: {} + sizeOfTupleDelimiter: {}",
            sizeOfBufferInBytes,
            offsetOfLastTupleDelimiter + sizeOfTupleDelimiter,
            offsetOfLastTupleDelimiter,
            sizeOfTupleDelimiter);
        const auto sizeOfTrailingSpanningTuple = sizeOfBufferInBytes - (offsetOfLastTupleDelimiter + sizeOfTupleDelimiter);
        const auto startOfTrailingSpanningTuple = offsetOfLastTupleDelimiter + sizeOfTupleDelimiter;
        return rawBuffer.getBufferView().substr(startOfTrailingSpanningTuple, sizeOfTrailingSpanningTuple);
    }

    [[nodiscard]] FieldIndex getOffsetOfLastTuple() const { return offsetOfFirstTupleDelimiter; }

    [[nodiscard]] FieldIndex getByteOffsetOfLastTuple() const { return offsetOfLastTupleDelimiter; }

    [[nodiscard]] size_t getSizeOfBufferInBytes() const { return this->sizeOfBufferInBytes; }

    [[nodiscard]] const RawTupleBuffer& getRawTupleBuffer() const { return rawBuffer; }

    void setSpanningTuple(const std::string_view spanningTuple) { rawBuffer.setSpanningTuple(spanningTuple); }


protected:
    RawTupleBuffer rawBuffer;
    size_t sizeOfBufferInBytes{};
    FieldIndex offsetOfFirstTupleDelimiter{};
    FieldIndex offsetOfLastTupleDelimiter{};
};

}
