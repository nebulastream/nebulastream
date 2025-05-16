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

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

Memory::AbstractBufferProvider* getBufferProviderProxy(const PipelineExecutionContext* pipelineCtx)
{
    return pipelineCtx->getBufferManager().get();
}

ExecutionContext::ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext, const nautilus::val<Arena*>& arena)
    : pipelineContext(pipelineContext)
    , pipelineMemoryProvider(arena, invoke(getBufferProviderProxy, pipelineContext))
    , originId(INVALID<OriginId>)
    , watermarkTs(0_u64)
    , currentTs(0_u64)
    , sequenceNumber(INVALID<SequenceNumber>)
    , chunkNumber(INVALID<ChunkNumber>)
    , lastChunk(true)
{
}

nautilus::val<Memory::TupleBuffer*> ExecutionContext::allocateBuffer() const
{
    auto bufferPtr = nautilus::invoke(
        +[](PipelineExecutionContext* pec)
        {
            PRECONDITION(pec, "pipeline execution context should not be null");
            /// We allocate a new tuple buffer for the runtime.
            /// As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
            /// This increases the reference counter in the buffer.
            /// When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
            const auto buffer = pec->allocateTupleBuffer();
            auto* tb = new Memory::TupleBuffer(buffer);
            return tb;
        },
        pipelineContext);
    return bufferPtr;
}

nautilus::val<int8_t*> ExecutionContext::allocateMemory(const nautilus::val<size_t>& sizeInBytes)
{
    return pipelineMemoryProvider.arena.allocateMemory(sizeInBytes);
}

void emitBufferProxy(PipelineExecutionContext* pipelineCtx, Memory::TupleBuffer* tb)
{
    NES_TRACE("Emitting buffer with SequenceData = {}", tb->getSequenceDataAsString());

    /* We have to emit all buffer, regardless of their number of tuples. This is due to the fact, that we expect all
     * sequence numbers to reach any operator. Sending empty buffers will have some overhead. As we are performing operator
     * fusion, this should only happen occasionally.
     */
    pipelineCtx->emitBuffer(*tb);

    /// delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tb;
}

void ExecutionContext::emitBuffer(const Nautilus::RecordBuffer& buffer) const
{
    nautilus::invoke(emitBufferProxy, pipelineContext, buffer.getReference());
}

WorkerThreadId getWorkerThreadIdProxy(const PipelineExecutionContext* pec)
{
    return pec->getId();
}

nautilus::val<WorkerThreadId> ExecutionContext::getWorkerThreadId() const
{
    return nautilus::invoke(getWorkerThreadIdProxy, pipelineContext);
}

OperatorState* ExecutionContext::getLocalState(OperatorId operatorId)
{
    const auto stateEntry = localStateMap.find(operatorId);
    INVARIANT(stateEntry != localStateMap.end(), "No local state registered for operator");
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(OperatorId operatorId, std::unique_ptr<OperatorState> state)
{
    INVARIANT(not localStateMap.contains(operatorId), "Operators state already registered for operator");
    localStateMap.emplace(operatorId, std::move(state));
}

static OperatorHandler* getGlobalOperatorHandlerProxy(PipelineExecutionContext* pipelineCtx, const OperatorHandlerId index)
{
    auto handlers = pipelineCtx->getOperatorHandlers();
    return handlers[index].get();
}

nautilus::val<OperatorHandler*> ExecutionContext::getGlobalOperatorHandler(const OperatorHandlerId handlerIndex) const
{
    const auto handlerIndexValue = nautilus::val<uint64_t>(handlerIndex.getRawValue());
    return nautilus::invoke(getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
}

}
