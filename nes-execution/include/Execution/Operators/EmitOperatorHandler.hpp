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
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution::Operators
{

/// Stores a sequenceNumber and an OriginId
struct SequenceNumberForOriginId
{
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    OriginId originId = INVALID_ORIGIN_ID;

    auto operator<=>(const SequenceNumberForOriginId&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const SequenceNumberForOriginId& obj)
    {
        return os << "{ seqNumber = " << obj.sequenceNumber << ", originId = " << obj.originId << "}";
    }
};

/// Container for storing information, related to the state of a sequence number
/// the SequenceState is only used inside 'folly::Synchronized' so its members do not need to be atomic themselves
struct SequenceState
{
    uint64_t lastChunkNumber = INVALID_CHUNK_NUMBER.getRawValue();
    uint64_t seenChunks = INVALID_CHUNK_NUMBER.getRawValue();
};

class EmitOperatorHandler final : public OperatorHandler
{
public:
    EmitOperatorHandler() = default;

    /// Returns the next chunk number belonging to a sequence number for emitting a buffer
    uint64_t getNextChunkNumber(SequenceNumberForOriginId seqNumberOriginId);

    /// Sets provided chunkNumber as lastChunkNumber if 'isLastChunk' flag is true. Otherwise, lastChunkNumber is set to an invalid chunkNumber.
    /// Checks if the number of seen chunks matches the lastChunkNumber.
    /// @return true, if the number of seenChunks matches the lastChunkNumber.
    bool processChunkNumber(SequenceNumberForOriginId seqNumberOriginId, ChunkNumber chunkNumber, bool isLastChunk);

    /// Removes the sequence state in seqNumberOriginIdToChunkStateInput and seqNumberOriginIdToOutputChunkNumber for the seqNumberOriginId
    void removeSequenceState(SequenceNumberForOriginId seqNumberOriginId);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    folly::Synchronized<std::map<SequenceNumberForOriginId, SequenceState>> seqNumberOriginIdToChunkStateInput;
    folly::Synchronized<std::map<SequenceNumberForOriginId, ChunkNumber::Underlying>> seqNumberOriginIdToOutputChunkNumber;
};
}

FMT_OSTREAM(NES::Runtime::Execution::Operators::SequenceNumberForOriginId);
