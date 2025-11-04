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
#include <map>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>

#ifndef NO_ASSERT
    #include <set>
#endif

namespace NES
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
    ChunkNumber::Underlying nextChunkNumberCounter = ChunkNumber::INITIAL;
    ChunkNumber lastChunkNumber = INVALID<ChunkNumber>;
    size_t seenChunks = 0;
};

class EmitOperatorHandler final : public OperatorHandler
{
public:
    void setChunkNumber(bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    folly::Synchronized<std::map<SequenceNumberForOriginId, SequenceState>> sequenceStates;

#ifndef NO_ASSERT
    /// We assume that every tuple of (SequenceNumber, ChunkNumber, OriginId) is unique per query.
    /// In debug mode we track the completed sequence numbers to catch bugs related to bad sequence/chunk numbers
    folly::Synchronized<std::set<SequenceNumberForOriginId>> completedSequences;
#endif
};
}

FMT_OSTREAM(NES::SequenceNumberForOriginId);
