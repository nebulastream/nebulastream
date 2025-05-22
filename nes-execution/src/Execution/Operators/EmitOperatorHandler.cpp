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

#include <cstdint>
#include <Execution/Operators/EmitOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators
{

uint64_t EmitOperatorHandler::getNextChunkNumber(const SequenceNumberForOriginId seqNumberOriginId)
{
    auto lockedMap = seqNumberOriginIdToOutputChunkNumber.wlock();
    const auto newChunkNumber = (*lockedMap)[seqNumberOriginId] + ChunkNumber::INITIAL;
    /// Increment the chunk number for the next chunk
    (*lockedMap)[seqNumberOriginId] += 1;
    return newChunkNumber;
}

void EmitOperatorHandler::removeSequenceState(const SequenceNumberForOriginId seqNumberOriginId)
{
    seqNumberOriginIdToOutputChunkNumber.wlock()->erase(seqNumberOriginId);
    seqNumberOriginIdToChunkStateInput.wlock()->erase(seqNumberOriginId);
}

void EmitOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}

void EmitOperatorHandler::stop(Runtime::QueryTerminationType, PipelineExecutionContext&)
{
}

bool EmitOperatorHandler::processChunkNumber(
    const SequenceNumberForOriginId seqNumberOriginId, const ChunkNumber chunkNumber, const bool isLastChunk)
{
    auto lockedMap = seqNumberOriginIdToChunkStateInput.wlock();
    auto& chunkState = (*lockedMap)[seqNumberOriginId];
    if (isLastChunk)
    {
        chunkState.lastChunkNumber = chunkNumber.getRawValue();
    }
    chunkState.seenChunks++;
    NES_TRACE(
        "seqNumberOriginId = {} chunkNumber = {} isLastChunk = {} seenChunks = {} lastChunkNumber = {}",
        seqNumberOriginId,
        chunkNumber,
        isLastChunk,
        chunkState.seenChunks,
        chunkState.lastChunkNumber)
    return chunkState.seenChunks == chunkState.lastChunkNumber;
}

}
