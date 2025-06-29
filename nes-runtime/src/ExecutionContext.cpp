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
#include <OperatorState.hpp>
#include <PipelineExecutionContext.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
Memory::AbstractBufferProvider* getBufferProviderProxy(const PipelineExecutionContext* pipelineCtx)
{
    return pipelineCtx->getBufferManager().get();
}
WorkerThreadId getWorkerThreadIdProxy(const PipelineExecutionContext* pec)
{
    return pec->getId();
}
}

int8_t* Arena::allocateMemory(const size_t sizeInBytes)
{
    lastAllocationOwnsBuffer = false;

    /// Case 1
    if (bufferProvider->getBufferSize() < sizeInBytes)
    {
        const auto unpooledBufferOpt = bufferProvider->getUnpooledBuffer(sizeInBytes);
        if (not unpooledBufferOpt.has_value())
        {
            throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size " + std::to_string(sizeInBytes));
        }
        unpooledBuffers.emplace_back(unpooledBufferOpt.value());
        lastAllocationSize = sizeInBytes;
        lastAllocationOwnsBuffer = true;
        return unpooledBuffers.back().getBuffer<int8_t>();
    }

    if (fixedSizeBuffers.empty())
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
        lastAllocationSize = bufferProvider->getBufferSize();
        return fixedSizeBuffers.back().getBuffer();
    }

    /// Case 2
    if (lastAllocationSize < currentOffset + sizeInBytes)
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
    }

    /// Case 3
    auto& lastBuffer = fixedSizeBuffers.back();
    lastAllocationSize = lastBuffer.getBufferSize();
    auto* const result = lastBuffer.getBuffer() + currentOffset;
    currentOffset += sizeInBytes;
    return result;
}

std::pair<nautilus::val<int8_t*>, nautilus::val<bool>> ArenaRef::allocateMemory(const nautilus::val<size_t>& sizeInBytes)
{
    nautilus::val<bool> ownsAllocation = false;
    /// If the available space for the pointer is smaller than the required size, we allocate a new buffer from the arena.
    /// We use the arena's allocateMemory function to allocate a new buffer and set the available space for the pointer to the last allocation size.
    /// Further, we set the space pointer to the beginning of the new buffer.
    if (availableSpaceForPointer < sizeInBytes)
    {
        spacePointer = nautilus::invoke(
            +[](Arena* arena, const size_t sizeInBytesVal) -> int8_t* { return arena->allocateMemory(sizeInBytesVal); },
            arenaRef,
            sizeInBytes);
        availableSpaceForPointer
            = Nautilus::Util::readValueFromMemRef<size_t>(Nautilus::Util::getMemberRef(arenaRef, &Arena::lastAllocationSize));
        ownsAllocation
            = Nautilus::Util::readValueFromMemRef<bool>(Nautilus::Util::getMemberRef(arenaRef, &Arena::lastAllocationOwnsBuffer));
    }
    availableSpaceForPointer -= sizeInBytes;
    auto result = spacePointer;
    spacePointer += sizeInBytes;
    return {result, ownsAllocation};
}

VariableSizedData ArenaRef::allocateVariableSizedData(const nautilus::val<size_t>& sizeInBytes)
{
    auto [basePtr, owns] = allocateMemory(sizeInBytes + nautilus::val<size_t>(4));
    *(static_cast<nautilus::val<uint32_t*>>(basePtr)) = sizeInBytes;
    return VariableSizedData(basePtr, sizeInBytes, VariableSizedData::Owned(owns));
}


ExecutionContext::ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext, const nautilus::val<Arena*>& arena)
    : pipelineContext(pipelineContext)
    , workerThreadId(nautilus::invoke(getWorkerThreadIdProxy, pipelineContext))
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
    return pipelineMemoryProvider.arena.allocateMemory(sizeInBytes).first;
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

OperatorState* ExecutionContext::getLocalState(const OperatorId operatorId)
{
    const auto stateEntry = localStateMap.find(operatorId);
    INVARIANT(stateEntry != localStateMap.end(), "No local state registered for operator");
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(const OperatorId operatorId, std::unique_ptr<OperatorState> state)
{
    localStateMap.insert_or_assign(operatorId, std::move(state));
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
