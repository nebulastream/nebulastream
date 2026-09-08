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
#include <ExecutionContext.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <nautilus/RuntimeBinding.hpp>
#include <nautilus/function.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <OperatorState.hpp>
#include <PipelineExecutionContext.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
AbstractBufferProvider* getBufferProviderProxy(const PipelineExecutionContext* pipelineCtx)
{
    return pipelineCtx->getBufferManager().get();
}

WorkerThreadId getWorkerThreadIdProxy(const PipelineExecutionContext* pec)
{
    return pec->getWorkerThreadId();
}

PipelineId getPipelineIdProxy(const PipelineExecutionContext* pec)
{
    return pec->getPipelineId();
}
}

ExecutionContext::ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext, const nautilus::val<Arena*>& arena)
    : pipelineContext(pipelineContext)
    , workerThreadId(nautilus::invoke(getWorkerThreadIdProxy, pipelineContext))
    , pipelineId(nautilus::invoke(getPipelineIdProxy, pipelineContext))
    , pipelineMemoryProvider(arena, invoke(getBufferProviderProxy, pipelineContext))
    , originId(INVALID<OriginId>)
    , watermarkTs(uint64_t{0})
    , currentTs(uint64_t{0})
    , sequenceNumber(INVALID<SequenceNumber>)
    , chunkNumber(INVALID<ChunkNumber>)
    , lastChunk(true)
{
}

NautilusBuffer ExecutionContext::allocateBuffer() const
{
    OwnedNautilusBuffer buffer;
    nautilus::invoke(
        +[](PipelineExecutionContext* pec, TupleBuffer* buffer)
        {
            PRECONDITION(pec, "pipeline execution context should not be null");
            *buffer = pec->allocateTupleBuffer();
        },
        pipelineContext,
        buffer.asArg());
    return buffer;
}

nautilus::val<int8_t*> ExecutionContext::allocateMemory(const nautilus::val<size_t>& sizeInBytes)
{
    return pipelineMemoryProvider.arena.allocateMemory(sizeInBytes);
}

void emitBufferProxy(PipelineExecutionContext* pipelineCtx, TupleBuffer* tb)
{
    NES_TRACE("Emitting buffer with SequenceData = {}", tb->getSequenceDataAsString());

    /* We have to emit all buffer, regardless of their number of tuples. This is due to the fact, that we expect all
     * sequence numbers to reach any operator. Sending empty buffers will have some overhead. As we are performing operator
     * fusion, this should only happen occasionally.
     */
    pipelineCtx->emitBuffer(*tb);
}

void ExecutionContext::emitBuffer(const RecordBuffer& buffer) const
{
    nautilus::invoke(emitBufferProxy, pipelineContext, buffer.getReference());
}

void ExecutionContext::setOpenReturnState(const OpenReturnState openReturnState)
{
    this->openReturnState = openReturnState;
}

OpenReturnState ExecutionContext::getOpenReturnState() const
{
    return this->openReturnState;
}

OperatorState* ExecutionContext::getLocalState(const OperatorId operatorId)
{
    const auto stateEntry = localStateMap.find(operatorId);
    INVARIANT(stateEntry != localStateMap.end(), "No local state registered for operator");
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(const OperatorId operatorId, std::unique_ptr<OperatorState> state)
{
    localStateMap.emplace(operatorId, std::move(state));
}

void ExecutionContext::registerOperatorHandler(CompilationContext& compilationContext, const OperatorHandlerId handlerId)
{
    if (!operatorHandlerBindings.contains(handlerId))
    {
        auto* handler = compilationContext.pipelineExecutionContext.getOperatorHandlers().at(handlerId).get();
        operatorHandlerBindings.emplace(
            handlerId,
            compilationContext.runtimeBindings.bind<OperatorHandler>(fmt::format("handler/{}", operatorHandlerBindings.size()), handler));
    }
}

nautilus::val<OperatorHandler*> ExecutionContext::getGlobalOperatorHandler(const OperatorHandlerId handlerIndex) const
{
    if (const auto binding = operatorHandlerBindings.find(handlerIndex); binding != operatorHandlerBindings.end())
    {
        return binding->second.get();
    }
#ifdef ENABLE_TRACING
    if (nautilus::tracing::inTracer())
    {
        throw std::logic_error("Operator handlers must be registered during setup before tracing");
    }
#endif
    auto* nativeContext = nautilus::details::RawValueResolver<PipelineExecutionContext*>::getRawValue(pipelineContext);
    if (nativeContext == nullptr)
    {
        throw std::logic_error("Missing native pipeline execution context");
    }
    const auto& handlers = nativeContext->getOperatorHandlers();
    const auto handler = handlers.find(handlerIndex);
    return nautilus::val<OperatorHandler*>(handler == handlers.end() ? nullptr : handler->second.get());
}

}
