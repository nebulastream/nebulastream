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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution
{

Memory::AbstractBufferProvider* getBufferProviderProxy(const PipelineExecutionContext* pipelineCtx)
{
    return pipelineCtx->getBufferManager().get();
}

ExecutionContext::ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext)
    : pipelineContext(pipelineContext)
    , bufferProvider(invoke(getBufferProviderProxy, pipelineContext))
    , originId(INVALID<OriginId>)
    , watermarkTs(0_u64)
    , currentTs(0_u64)
    , sequenceNumber(INVALID<SequenceNumber>)
    , chunkNumber(INVALID<ChunkNumber>)
    , lastChunk(true)
{
}

Memory::TupleBuffer* allocateBufferProxy(PipelineExecutionContext* pec)
{
    PRECONDITION(pec, "pipeline execution context should not be null");
    /// We allocate a new tuple buffer for the runtime.
    /// As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    /// This increases the reference counter in the buffer.
    /// When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    auto buffer = pec->allocateTupleBuffer();
    auto* tb = new Memory::TupleBuffer(buffer);
    return tb;
}

nautilus::val<Memory::TupleBuffer*> ExecutionContext::allocateBuffer() const
{
    auto bufferPtr = nautilus::invoke(allocateBufferProxy, pipelineContext);
    return bufferPtr;
}

nautilus::val<Memory::AbstractBufferProvider*> ExecutionContext::getBufferProvider() const
{
    return nautilus::invoke(+[](PipelineExecutionContext* pec) { return pec->getBufferManager().get(); }, pipelineContext);
}

void emitBufferProxy(PipelineExecutionContext* pipelineCtx, Memory::TupleBuffer* tb)
{
    NES_TRACE("Emitting buffer with SequenceData = {}", tb->getSequenceDataAsString());

    /* We have to emit all buffer, regardless of their number of tuples. This is due to the fact, that we expect all
     * sequence numbers to reach any operator. Sending empty buffers will have some overhead. As we are performing operator
     * fusion, this should only happen occasionally.
     */
    pipelineCtx->emitBuffer(*tb, PipelineExecutionContext::ContinuationPolicy::NEVER);

    /// delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tb;
}

void ExecutionContext::emitBuffer(const RecordBuffer& buffer) const
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

Operators::OperatorState* ExecutionContext::getLocalState(const Operators::Operator* op)
{
    const auto stateEntry = localStateMap.find(op);
    INVARIANT(stateEntry != localStateMap.end(), "No local state registered for operator");
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state)
{
    INVARIANT(not localStateMap.contains(op), "Operators state already registered for operator");
    localStateMap.emplace(op, std::move(state));
}

OperatorHandler* getGlobalOperatorHandlerProxy(PipelineExecutionContext* pipelineCtx, const uint64_t index)
{
    auto handlers = pipelineCtx->getOperatorHandlers();
    auto size = handlers.size();
    PRECONDITION(index < size, "operator handler at index {} is not registered", index);
    return handlers[index].get();
}

nautilus::val<OperatorHandler*> ExecutionContext::getGlobalOperatorHandler(const uint64_t handlerIndex) const
{
    const auto handlerIndexValue = nautilus::val<uint64_t>(handlerIndex);
    return nautilus::invoke(getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
}

}
