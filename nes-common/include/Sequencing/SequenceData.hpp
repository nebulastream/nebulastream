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
#include <ostream>
#include <sstream>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Position of a buffer in the sequence of its origin. It covers [`sequenceNumber` - `sequenceRangeOffset`, `sequenceNumber`],
/// and `chunkNumber` and `lastChunk` refer to the whole range.
struct SequenceData
{
    SequenceData(SequenceNumber sequenceNumber, uint32_t sequenceRangeOffset, ChunkNumber chunkNumber, bool lastChunk);
    explicit SequenceData();

    friend std::ostream& operator<<(std::ostream& os, const SequenceData& obj);

    /// Checks sequenceNumber, then chunkNumber, then lastChunk, then sequenceRangeOffset
    friend auto operator<=>(const SequenceData& lhs, const SequenceData& rhs) = default;

    SequenceNumber::Underlying sequenceNumber;
    ChunkNumber::Underlying chunkNumber;
    bool lastChunk;
    uint32_t sequenceRangeOffset;
};

}

FMT_OSTREAM(NES::SequenceData);
