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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
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

nautilus::val<int8_t*> ExecutionContext::allocateMemory(const nautilus::val<size_t>& sizeInBytes)
{
    return pipelineMemoryProvider.arena.allocateMemory(sizeInBytes);
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
