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

#include <EmitOperatorHandler.hpp>

#include <cstdint>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

void EmitOperatorHandler::setChunkNumber(
    bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer)
{
    /// The SequenceState tracks how many chunks have been seen per Sequence/OriginId pair.
    /// This allows to reason about completeness of a SequenceNumber and assign unique ChunkNumbers to every produced buffer aswell
    /// as assigning exactly one LastChunk flag once we have seen all chunks.

    SequenceNumberForOriginId seqNumberOriginId(buffer.getSequenceNumber(), buffer.getOriginId());
    const auto lock = sequenceStates.wlock();

    /// Assign a unique chunk number to the buffer and increment the internal counter
    auto& [nextChunkNumberCounter, lastChunkNumber, seenChunks] = (*lock)[seqNumberOriginId];

#ifndef NO_ASSERT
    const auto completedSequencesLock = completedSequences.wlock();
    INVARIANT(
        !completedSequencesLock->contains(seqNumberOriginId),
        "Received chunk for sequence {} that was already completed",
        seqNumberOriginId);
#endif


    const auto nextChunkNumber = ChunkNumber(nextChunkNumberCounter);
    nextChunkNumberCounter += 1;

    buffer.setChunkNumber(nextChunkNumber);
    buffer.setLastChunk(false);

    /// Check for completeness of the sequence number. This is only relevant if this buffer is actually completing an incoming chunk.
    if (isEndOfIncomingChunk)
    {
        /// As soon as the incomingBufferLastChunk is received we know how many incoming chunks to expect.
        if (isIncomingBufferTheLastChunk)
        {
            INVARIANT(
                lastChunkNumber == INVALID<ChunkNumber>,
                "Received multiple last chunks for {}. Previous last chunk was {}",
                seqNumberOriginId,
                lastChunkNumber);
            lastChunkNumber = incomingChunkNumber;
        }
        seenChunks++;

        /// We have processed the expected number of chunks and this buffer is closing the input tuple buffer, so we assign the last flag
        /// and erase the sequenceState.
        if (lastChunkNumber != INVALID<ChunkNumber> && seenChunks - 1 == lastChunkNumber.getRawValue() - ChunkNumber::INITIAL)
        {
            lock->erase(seqNumberOriginId);
#ifndef NO_ASSERT
            completedSequencesLock->emplace(seqNumberOriginId);
#endif
            buffer.setLastChunk(true);
        }
    }
}

void EmitOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}

void EmitOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

}
