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
ExecutionContext::ExecutionContext(const nautilus::val<int8_t*>& workerContext, const nautilus::val<int8_t*>& pipelineContext)
    : workerContext(workerContext)
    , pipelineContext(pipelineContext)
    , origin(0_u64)
    , watermarkTs(0_u64)
    , currentTs(0_u64)
    , sequenceNumber(0_u64)
    , chunkNumber(0_u64)
    , lastChunk(true)
{
}

void* allocateBufferProxy(void* workerContextPtr)
{
    if (workerContextPtr == nullptr)
    {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    const auto wkrCtx = static_cast<Runtime::WorkerContext*>(workerContextPtr);
    /// We allocate a new tuple buffer for the runtime.
    /// As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    /// This increases the reference counter in the buffer.
    /// When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    auto buffer = wkrCtx->allocateTupleBuffer();
    auto* tb = new Memory::TupleBuffer(buffer);
    return tb;
}

nautilus::val<int8_t*> ExecutionContext::allocateBuffer()
{
    auto bufferPtr = nautilus::invoke(allocateBufferProxy, workerContext);
    return bufferPtr;
}

void emitBufferProxy(void* wc, void* pc, void* tupleBuffer)
{
    auto* tb = (Memory::TupleBuffer*)tupleBuffer;
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(pc);
    auto workerCtx = static_cast<WorkerContext*>(wc);

    NES_TRACE("Emitting buffer with SequenceData = {}", tb->getSequenceData().toString());

    /* We have to emit all buffer, regardless of their number of tuples. This is due to the fact, that we expect all
     * sequence numbers to reach any operator. Sending empty buffers will have some overhead. As we are performing operator
     * fusion, this should only happen occasionally.
     */
    pipelineCtx->emitBuffer(*tb, *workerCtx);

    /// delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tb;
}

void ExecutionContext::emitBuffer(const RecordBuffer& buffer)
{
    nautilus::invoke(emitBufferProxy, workerContext, pipelineContext, buffer.getReference());
}

WorkerThreadId getWorkerThreadIdProxy(void* workerContext)
{
    auto* wc = static_cast<Runtime::WorkerContext*>(workerContext);
    return wc->getId();
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

void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index)
{
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(pc);
    auto handlers = pipelineCtx->getOperatorHandlers();
    auto size = handlers.size();
    if (index >= size)
    {
        NES_THROW_RUNTIME_ERROR("operator handler at index " + std::to_string(index) + " is not registered");
    }
    return handlers[index].get();
}

nautilus::val<int8_t*> ExecutionContext::getGlobalOperatorHandler(uint64_t handlerIndex)
{
    const auto handlerIndexValue = nautilus::val<uint64_t>(handlerIndex);
    return nautilus::invoke(getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
}

const nautilus::val<int8_t*>& ExecutionContext::getWorkerContext() const
{
    return workerContext;
}
const nautilus::val<int8_t*>& ExecutionContext::getPipelineContext() const
{
    return pipelineContext;
}

const nautilus::val<uint64_t>& ExecutionContext::getWatermarkTs() const
{
    return watermarkTs;
}

void ExecutionContext::setWatermarkTs(nautilus::val<uint64_t> watermarkTs)
{
    this->watermarkTs = watermarkTs;
}

const nautilus::val<uint64_t>& ExecutionContext::getSequenceNumber() const
{
    return sequenceNumber;
}

void ExecutionContext::setSequenceNumber(const nautilus::val<uint64_t>& sequenceNumber)
{
    this->sequenceNumber = sequenceNumber;
}

const nautilus::val<uint64_t>& ExecutionContext::getOriginId() const
{
    return origin;
}

void ExecutionContext::setOriginId(const nautilus::val<uint64_t>& origin)
{
    this->origin = origin;
}

const nautilus::val<uint64_t>& ExecutionContext::getCurrentTs() const
{
    return currentTs;
}

void ExecutionContext::setCurrentTs(const nautilus::val<uint64_t>& currentTs)
{
    this->currentTs = currentTs;
}

const nautilus::val<uint64_t>& ExecutionContext::getChunkNumber() const
{
    return chunkNumber;
}

void ExecutionContext::setChunkNumber(const nautilus::val<uint64_t>& chunkNumber)
{
    this->chunkNumber = chunkNumber;
}

const nautilus::val<bool>& ExecutionContext::getLastChunk() const
{
    return lastChunk;
}

void ExecutionContext::setLastChunk(const nautilus::val<bool>& lastChunk)
{
    this->lastChunk = lastChunk;
}

uint64_t getNextChunkNumberProxy(void* ptrPipelineCtx, uint64_t originId, uint64_t sequenceNumber)
{
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    return pipelineCtx->getNextChunkNumber({SequenceNumber(sequenceNumber), OriginId(originId)});
}

bool isLastChunkProxy(void* ptrPipelineCtx, uint64_t originId, uint64_t sequenceNumber, uint64_t chunkNumber, bool isLastChunk)
{
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    return pipelineCtx->isLastChunk({SequenceNumber(sequenceNumber), OriginId(originId)}, ChunkNumber(chunkNumber), isLastChunk);
}

void removeSequenceStateProxy(void* ptrPipelineCtx, uint64_t originId, uint64_t sequenceNumber)
{
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    pipelineCtx->removeSequenceState({SequenceNumber(sequenceNumber), OriginId(originId)});
}

nautilus::val<bool> ExecutionContext::isLastChunk() const
{
    return nautilus::invoke(
        isLastChunkProxy,
        this->getPipelineContext(),
        this->getOriginId(),
        this->getSequenceNumber(),
        this->getChunkNumber(),
        this->getLastChunk());
}

nautilus::val<uint64_t> ExecutionContext::getNextChunkNr() const
{
    return nautilus::invoke(getNextChunkNumberProxy, this->getPipelineContext(), this->getOriginId(), this->getSequenceNumber());
}

void ExecutionContext::removeSequenceState() const
{
    nautilus::invoke(removeSequenceStateProxy, this->getPipelineContext(), this->getOriginId(), this->getSequenceNumber());
}

} /// namespace NES::Runtime::Execution
