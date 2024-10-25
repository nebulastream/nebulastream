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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>

namespace NES::Runtime::Execution
{
ExecutionContext::ExecutionContext(
    const nautilus::val<WorkerContext*>& workerContext, const nautilus::val<PipelineExecutionContext*>& pipelineContext)
    : workerContext(workerContext)
    , pipelineContext(pipelineContext)
    , originId(0_u64)
    , watermarkTs(0_u64)
    , currentTs(0_u64)
    , sequenceNumber(0_u64)
    , chunkNumber(0_u64)
    , lastChunk(true)
{
}

Memory::TupleBuffer* allocateBufferProxy(WorkerContext* workerContext)
{
    if (workerContext == nullptr)
    {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    /// We allocate a new tuple buffer for the runtime.
    /// As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    /// This increases the reference counter in the buffer.
    /// When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    const auto buffer = workerContext->allocateTupleBuffer();
    auto* tb = new Memory::TupleBuffer(buffer);
    return tb;
}

nautilus::val<Memory::TupleBuffer*> ExecutionContext::allocateBuffer()
{
    auto bufferPtr = nautilus::invoke(allocateBufferProxy, workerContext);
    return bufferPtr;
}

void emitBufferProxy(WorkerContext* workerContext, PipelineExecutionContext* pipelineCtx, Memory::TupleBuffer* tupleBuffer)
{
    NES_TRACE("Emitting buffer with SequenceData = {}", tupleBuffer->getSequenceDataAsString());

    /* We have to emit all buffer, regardless of their number of tuples. This is due to the fact, that we expect all
     * sequence numbers to reach any operator. Sending empty buffers will have some overhead. As we are performing operator
     * fusion, this should only happen occasionally.
     */
    pipelineCtx->emitBuffer(*tupleBuffer, *workerContext);

    /// delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tupleBuffer;
}

void ExecutionContext::emitBuffer(const RecordBuffer& buffer)
{
    nautilus::invoke(emitBufferProxy, workerContext, pipelineContext, buffer.getReference());
}

WorkerThreadId getWorkerThreadIdProxy(const WorkerContext* workerContext)
{
    return workerContext->getId();
}

nautilus::val<WorkerThreadId> ExecutionContext::getWorkerThreadId()
{
    return nautilus::invoke(getWorkerThreadIdProxy, workerContext);
}

Operators::OperatorState* ExecutionContext::getLocalState(const Operators::Operator* op)
{
    const auto stateEntry = localStateMap.find(op);
    if (stateEntry == localStateMap.end())
    {
        NES_THROW_RUNTIME_ERROR("No local state registered for operator: " << op);
    }
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state)
{
    if (localStateMap.contains(op))
    {
        NES_THROW_RUNTIME_ERROR("Operators state already registered for operator: " << op);
    }
    localStateMap.emplace(op, std::move(state));
}

OperatorHandler* getGlobalOperatorHandlerProxy(PipelineExecutionContext* pipelineCtx, const uint64_t index)
{
    auto handlers = pipelineCtx->getOperatorHandlers();
    auto size = handlers.size();
    if (index >= size)
    {
        NES_THROW_RUNTIME_ERROR("operator handler at index " + std::to_string(index) + " is not registered");
    }
    return handlers[index].get();
}

nautilus::val<OperatorHandler*> ExecutionContext::getGlobalOperatorHandler(uint64_t handlerIndex)
{
    const auto handlerIndexValue = nautilus::val<uint64_t>(handlerIndex);
    return nautilus::invoke(getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
}

ChunkNumber::Underlying
getNextChunkNumberProxy(PipelineExecutionContext* pipelineCtx, OriginId::Underlying originId, SequenceNumber::Underlying sequenceNumber)
{
    NES_ASSERT2_FMT(pipelineCtx != nullptr, "operator handler should not be null");
    return pipelineCtx->getNextChunkNumber({SequenceNumber(sequenceNumber), OriginId(originId)});
}

bool isLastChunkProxy(
    PipelineExecutionContext* pipelineCtx,
    OriginId::Underlying originId,
    SequenceNumber::Underlying sequenceNumber,
    ChunkNumber::Underlying chunkNumber,
    bool isLastChunk)
{
    NES_ASSERT2_FMT(pipelineCtx != nullptr, "operator handler should not be null");
    return pipelineCtx->isLastChunk({SequenceNumber(sequenceNumber), OriginId(originId)}, ChunkNumber(chunkNumber), isLastChunk);
}

void removeSequenceStateProxy(
    PipelineExecutionContext* pipelineCtx, OriginId::Underlying originId, SequenceNumber::Underlying sequenceNumber)
{
    NES_ASSERT2_FMT(pipelineCtx != nullptr, "operator handler should not be null");
    pipelineCtx->removeSequenceState({SequenceNumber(sequenceNumber), OriginId(originId)});
}

nautilus::val<bool> ExecutionContext::isLastChunk() const
{
    return nautilus::invoke(
        isLastChunkProxy, this->pipelineContext, this->originId, this->sequenceNumber, this->chunkNumber, this->lastChunk);
}

nautilus::val<ChunkNumber> ExecutionContext::getNextChunkNumber() const
{
    return {nautilus::invoke(getNextChunkNumberProxy, this->pipelineContext, this->originId, this->sequenceNumber)};
}

void ExecutionContext::removeSequenceState() const
{
    nautilus::invoke(removeSequenceStateProxy, this->pipelineContext, this->originId, this->sequenceNumber);
}

}
