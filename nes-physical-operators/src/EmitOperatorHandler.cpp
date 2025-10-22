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
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

void EmitOperatorHandler::setChunkNumber(
    bool chunkCompleted, ChunkNumber currentChunkNumber, bool isCurrentBufferTheLastChunk, TupleBuffer& buffer)
{
    SequenceNumberForOriginId seqNumberOriginId(buffer.getSequenceNumber(), buffer.getOriginId());
    auto chunkNumberCounter = seqNumberOriginIdToOutputChunkNumber.wlock();
    const auto newChunkNumber = ChunkNumber((*chunkNumberCounter)[seqNumberOriginId]++ + ChunkNumber::INITIAL);
    buffer.setChunkNumber(newChunkNumber);
    buffer.setLastChunk(false);
    if (chunkCompleted)
    {
        auto chunkStateInput = seqNumberOriginIdToChunkStateInput.wlock();
        auto& [lastChunkNumber, seenChunks] = (*chunkStateInput)[seqNumberOriginId];
        if (isCurrentBufferTheLastChunk)
        {
            lastChunkNumber = currentChunkNumber.getRawValue();
        }
        seenChunks++;
        if (seenChunks == lastChunkNumber)
        {
            chunkNumberCounter->erase(seqNumberOriginId);
            chunkStateInput->erase(seqNumberOriginId);
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
