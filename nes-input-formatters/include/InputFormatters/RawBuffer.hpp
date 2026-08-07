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
#include <Runtime/Buffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

using FieldIndex = uint32_t;
using SequenceNumberType = SequenceNumber::Underlying;

/// Wraps a Buffer that contains raw, unformatted data. Exposes a string_view over the payload for indexers/parsers while keeping
/// the underlying Buffer reachable only to classes that legitimately need it (e.g. SpanningTupleBufferState).
class RawBuffer
{
public:
    RawBuffer() = default;
    ~RawBuffer() = default;
    explicit RawBuffer(Buffer buffer)
        : rawBuffer(std::move(buffer)), bufferView(rawBuffer.getAvailableMemoryArea<char>().data(), rawBuffer.getNumberOfTuples()) { };

    RawBuffer(RawBuffer&& other) noexcept = default;
    RawBuffer& operator=(RawBuffer&& other) noexcept = default;
    RawBuffer(const RawBuffer& other) = default;
    RawBuffer& operator=(const RawBuffer& other) = default;

    [[nodiscard]] size_t getNumberOfBytes() const noexcept { return rawBuffer.getNumberOfTuples(); }

    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept { return rawBuffer.getSequenceNumber(); }

    [[nodiscard]] std::string_view getBufferView() const noexcept { return bufferView; }

    [[nodiscard]] const Buffer& getRawBuffer() const noexcept { return rawBuffer; }

private:
    Buffer rawBuffer;
    std::string_view bufferView;
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

    StagedBuffer(RawBuffer rawBuffer, const uint32_t offsetOfFirstTupleDelimiter, const uint32_t offsetOfLastTupleDelimiter)
        : rawBuffer(std::move(rawBuffer))
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
        /// The start-of-stream sentinel that anchors the first leading spanning tuple owns no buffer (empty view).
        /// It contributes no trailing bytes, so return early before the (delimiter-size-based) bounds check below.
        if (rawBuffer.getBufferView().empty())
        {
            return {};
        }
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

    [[nodiscard]] const RawBuffer& getRawBuffer() const { return rawBuffer; }

protected:
    RawBuffer rawBuffer;
    size_t sizeOfBufferInBytes{};
    FieldIndex offsetOfFirstTupleDelimiter{};
    FieldIndex offsetOfLastTupleDelimiter{};
};

}
