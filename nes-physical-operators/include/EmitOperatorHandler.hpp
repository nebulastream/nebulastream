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
#include <CompilationContext.hpp>
#include <PipelineExecutionContext.hpp>

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

/// Assigns the chunk numbers of the buffers an `EmitPhysicalOperator` emits, such that the outputs of every sequence number carry the
/// chunk numbers 1 to n and only chunk n is marked last. Subclasses decide what happens to the complete output of an input.
class EmitOperatorHandler : public OperatorHandler
{
public:
    /// Called by the emit's `setup` when its pipeline starts, before the pipeline is compiled.
    virtual void setup(CompilationContext&, PipelineExecutionContext&) { }

    void start(PipelineExecutionContext& pipelineExecutionContext) final;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// Emits a full buffer that the input with `incomingChunkNumber` filled before it ended.
    void emitSpilled(TupleBuffer& buffer, ChunkNumber incomingChunkNumber, PipelineExecutionContext& pec);

    /// Emits the last output of the input with `incomingChunkNumber`. `spilled` tells whether the input emitted full buffers before.
    void emitClosing(
        TupleBuffer& buffer,
        bool spilled,
        ChunkNumber incomingChunkNumber,
        bool isIncomingBufferTheLastChunk,
        PipelineExecutionContext& pec);

protected:
    /// Emits `output`, the only output of its sequence number, as its only chunk.
    virtual void emitCompleteOutput(TupleBuffer& output, PipelineExecutionContext& pec);

private:
    void setChunkNumber(bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer);

    folly::Synchronized<std::map<SequenceNumberForOriginId, SequenceState>> sequenceStates;

#ifndef NO_ASSERT
    /// We assume that every tuple of (SequenceNumber, ChunkNumber, OriginId) is unique per query.
    /// In debug mode we track the completed sequence numbers to catch bugs related to bad sequence/chunk numbers
    folly::Synchronized<std::set<SequenceNumberForOriginId>> completedSequences;
#endif
};
}

FMT_OSTREAM(NES::SequenceNumberForOriginId);
