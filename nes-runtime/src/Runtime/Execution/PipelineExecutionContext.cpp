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

#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES
{

PipelineExecutionContext::PipelineExecutionContext(
    PipelineId pipelineId,
    QueryId queryId,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
    size_t numberOfWorkerThreads,
    std::function<void(Memory::TupleBuffer&, WorkerContextRef)>&& emitFunction,
    std::function<void(Memory::TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers)
    : pipelineId(pipelineId)
    , queryId(queryId)
    , emitFunctionHandler(std::move(emitFunction))
    , emitToQueryManagerFunctionHandler(std::move(emitToQueryManagerFunctionHandler))
    , operatorHandlers(std::move(operatorHandlers))
    , bufferProvider(bufferProvider)
    , numberOfWorkerThreads(numberOfWorkerThreads)
{
}

void PipelineExecutionContext::emitBuffer(Memory::TupleBuffer& buffer, WorkerContextRef workerContext)
{
    /// call the function handler
    emitFunctionHandler(buffer, workerContext);
}

void PipelineExecutionContext::dispatchBuffer(Memory::TupleBuffer buffer)
{
    /// call the function handler
    emitToQueryManagerFunctionHandler(buffer);
}

std::vector<std::shared_ptr<OperatorHandler>> PipelineExecutionContext::getOperatorHandlers()
{
    return operatorHandlers;
}

std::string PipelineExecutionContext::toString() const
{
    return fmt::format("PipelineContext(queryID: {})", queryId);
}
uint64_t PipelineExecutionContext::getNumberOfWorkerThreads() const
{
    return numberOfWorkerThreads;
}
std::shared_ptr<Memory::AbstractBufferProvider> PipelineExecutionContext::getBufferManager() const
{
    return bufferProvider;
}

ChunkNumber::Underlying PipelineExecutionContext::getNextChunkNumber(const SeqNumberOriginId seqNumberOriginId)
{
    auto lockedMap = seqNumberOriginIdToOutputChunkNumber.wlock();
    (*lockedMap)[seqNumberOriginId] += 1;
    return (*lockedMap)[seqNumberOriginId];
}

void PipelineExecutionContext::removeSequenceState(const SeqNumberOriginId seqNumberOriginId)
{
    seqNumberOriginIdToOutputChunkNumber.wlock()->erase(seqNumberOriginId);
    seqNumberOriginIdToChunkStateInput.wlock()->erase(seqNumberOriginId);
}

bool PipelineExecutionContext::isLastChunk(const SeqNumberOriginId seqNumberOriginId, const ChunkNumber chunkNumber, const bool isLastChunk)
{
    const auto lockedMap = seqNumberOriginIdToChunkStateInput.wlock();
    auto& [lastChunkNumber, seenChunks] = (*lockedMap)[seqNumberOriginId];
    if (isLastChunk)
    {
        lastChunkNumber = chunkNumber.getRawValue();
    }
    seenChunks++;
    NES_TRACE(
        "seqNumberOriginId = {} chunkNumber = {} isLastChunk = {} seenChunks = {} lastChunkNumber = {}",
        seqNumberOriginId.toString(),
        chunkNumber,
        isLastChunk,
        seenChunks,
        lastChunkNumber)
    return seenChunks == lastChunkNumber;
}

}
