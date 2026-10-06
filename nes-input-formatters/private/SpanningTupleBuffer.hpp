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

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <ostream>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>

namespace NES
{

/// Resolves records spanning raw buffers. Empty raw buffers occupy sequence positions but hold no
/// bytes, so consecutive positions are stored as one interval and never retain pooled buffers.
/// A mutex makes claiming a spanning tuple and releasing its staged buffers atomic across workers.
class SpanningTupleBuffer
{
    struct Entry
    {
        StagedBuffer leading;
        StagedBuffer trailing;
        bool hasDelimiter;
        bool leadingUsed;
        bool trailingUsed;
    };

public:
    SpanningTupleBuffer();

    [[nodiscard]] SequenceShredderResult
    tryFindLeadingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);
    [[nodiscard]] SpanningBuffers tryFindTrailingSpanningTupleForBufferWithDelimiter(SequenceNumber sequenceNumber);
    [[nodiscard]] SequenceShredderResult
    tryFindSpanningTupleForBufferWithoutDelimiter(SequenceNumber sequenceNumber, const StagedBuffer& indexedRawBuffer);

    [[nodiscard]] bool validate() const;
    [[nodiscard]] SequenceShredder::Snapshot snapshot() const;
    void restore(SequenceShredder::Snapshot snapshot);

    friend std::ostream& operator<<(std::ostream& os, const SpanningTupleBuffer& sequenceRingBuffer);

private:
    mutable std::mutex mutex;
    std::map<uint64_t, Entry> entries;
    std::map<uint64_t, uint64_t> emptyRanges;

    [[nodiscard]] bool isEmptySequence(uint64_t sequenceNumber) const;
    [[nodiscard]] bool insertEmptySequence(uint64_t sequenceNumber);
    [[nodiscard]] bool insertBuffer(uint64_t sequenceNumber, const StagedBuffer& indexedRawBuffer, bool hasDelimiter);
    [[nodiscard]] std::optional<uint64_t> findPreviousDelimiter(uint64_t sequenceNumber) const;
    [[nodiscard]] std::optional<uint64_t> findNextDelimiter(uint64_t sequenceNumber) const;
    [[nodiscard]] SpanningBuffers claimSpan(uint64_t firstSequenceNumber, uint64_t lastSequenceNumber);
    void eraseEmptySequences(uint64_t firstSequenceNumber, uint64_t lastSequenceNumber);
};

}

FMT_OSTREAM(NES::SpanningTupleBuffer);
