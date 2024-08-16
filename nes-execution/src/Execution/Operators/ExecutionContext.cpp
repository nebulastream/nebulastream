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
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution {

ExecutionContext::ExecutionContext(const Nautilus::MemRefVal& workerContext, const Nautilus::MemRefVal& pipelineContext)
    : workerContext(workerContext), pipelineContext(pipelineContext), origin(0_u64), statisticId(INVALID_STATISTIC_ID),
      watermarkTs(Nautilus::ExecDataUInt64::create(0)), currentTs(Nautilus::ExecDataUInt64::create(0)), sequenceNumber(0_u64), chunkNumber(0_u64), lastChunk(true) {}

void* allocateBufferProxy(void* workerContextPtr) {
    if (workerContextPtr == nullptr) {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    auto wkrCtx = static_cast<Runtime::WorkerContext*>(workerContextPtr);
    // We allocate a new tuple buffer for the runtime.
    // As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    // This increases the reference counter in the buffer.
    // When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    auto buffer = wkrCtx->allocateTupleBuffer();
    auto* tb = new Runtime::TupleBuffer(buffer);
    return tb;
}

Nautilus::MemRefVal ExecutionContext::allocateBuffer() {
    auto bufferPtr = invoke(allocateBufferProxy, workerContext);
    return bufferPtr;
}

void emitBufferProxy(void* wc, void* pc, void* tupleBuffer) {
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(pc);
    auto workerCtx = static_cast<WorkerContext*>(wc);

    NES_TRACE("Emitting buffer with SequenceData = {}", tb->getSequenceData().toString());

    /* We have to emit all buffer, regardless of their number of tuples. This is due to the fact, that we expect all
     * sequence numbers to reach any operator. Sending empty buffers will have some overhead. As we are performing operator
     * fusion, this should only happen occasionally.
     */
    pipelineCtx->dispatchBuffer(*tb);

    // delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tb;
}

void ExecutionContext::emitBuffer(const NES::Runtime::Execution::RecordBuffer& buffer) {
    invoke(emitBufferProxy, workerContext, pipelineContext, buffer.getReference());
}

uint32_t getWorkerThreadIdProxy(void* workerContext) {
    auto* wc = static_cast<Runtime::WorkerContext*>(workerContext);
    return wc->getId().getRawValue();
}

Nautilus::UInt32Val ExecutionContext::getWorkerThreadId() { return invoke(getWorkerThreadIdProxy, workerContext); }

Operators::OperatorState* ExecutionContext::getLocalState(const Operators::Operator* op) {
    auto stateEntry = localStateMap.find(op);
    if (stateEntry == localStateMap.end()) {
        NES_THROW_RUNTIME_ERROR("No local state registered for operator: " << op);
    }
    return stateEntry->second.get();
}

void ExecutionContext::setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state) {
    if (localStateMap.contains(op)) {
        NES_THROW_RUNTIME_ERROR("Operators state already registered for operator: " << op);
    }
    localStateMap.emplace(op, std::move(state));
}

void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index) {
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(pc);
    auto handlers = pipelineCtx->getOperatorHandlers();
    auto size = handlers.size();
    if (index >= size) {
        NES_THROW_RUNTIME_ERROR("operator handler at index " + std::to_string(index) + " is not registered");
    }
    return handlers[index].get();
}

Nautilus::MemRefVal ExecutionContext::getGlobalOperatorHandler(uint64_t handlerIndex) {
    Nautilus::UInt64Val handlerIndexValue = Nautilus::UInt64Val(handlerIndex);
    return invoke(getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
}

const Nautilus::MemRefVal& ExecutionContext::getWorkerContext() const { return workerContext; }
const Nautilus::MemRefVal& ExecutionContext::getPipelineContext() const { return pipelineContext; }

const Nautilus::ExecDataType& ExecutionContext::getWatermarkTs() const { return watermarkTs; }

void ExecutionContext::setWatermarkTs(const Nautilus::ExecDataType& watermarkTs) { this->watermarkTs = watermarkTs; }

const Nautilus::UInt64Val& ExecutionContext::getSequenceNumber() const { return sequenceNumber; }

void ExecutionContext::setSequenceNumber(Nautilus::UInt64Val sequenceNumber) { this->sequenceNumber = sequenceNumber; }

const Nautilus::UInt64Val& ExecutionContext::getOriginId() const { return origin; }

const Nautilus::UInt64Val& ExecutionContext::getCurrentStatisticId() const { return statisticId; }

void ExecutionContext::setOrigin(Nautilus::UInt64Val origin) { this->origin = origin; }

void ExecutionContext::setCurrentStatisticId(Nautilus::UInt64Val statisticId) { this->statisticId = statisticId; }

const Nautilus::ExecDataType& ExecutionContext::getCurrentTs() const { return currentTs; }

void ExecutionContext::setCurrentTs(const Nautilus::ExecDataType& currentTs) { this->currentTs = currentTs; }

const Nautilus::UInt64Val& ExecutionContext::getChunkNumber() const { return chunkNumber; }

void ExecutionContext::setChunkNumber(Nautilus::UInt64Val chunkNumber) { this->chunkNumber = chunkNumber; }

const Nautilus::BooleanVal& ExecutionContext::getLastChunk() const { return lastChunk; }

void ExecutionContext::setLastChunk(Nautilus::BooleanVal lastChunk) { this->lastChunk = lastChunk; }

uint64_t getNextChunkNumberProxy(void* ptrPipelineCtx, uint64_t originId, uint64_t sequenceNumber) {
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    return pipelineCtx->getNextChunkNumber({SequenceNumber(sequenceNumber), OriginId(originId)});
}

bool isLastChunkProxy(void* ptrPipelineCtx, uint64_t originId, uint64_t sequenceNumber, uint64_t chunkNumber, bool isLastChunk) {
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    return pipelineCtx->isLastChunk({SequenceNumber(sequenceNumber), OriginId(originId)}, ChunkNumber(chunkNumber), isLastChunk);
}

void removeSequenceStateProxy(void* ptrPipelineCtx, uint64_t originId, uint64_t sequenceNumber) {
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    pipelineCtx->removeSequenceState({SequenceNumber(sequenceNumber), OriginId(originId)});
}

Nautilus::BooleanVal ExecutionContext::isLastChunk() const {
    return invoke(isLastChunkProxy,
                  this->getPipelineContext(),
                  this->getOriginId(),
                  this->getSequenceNumber(),
                  this->getChunkNumber(),
                  this->getLastChunk());
}

Nautilus::UInt64Val ExecutionContext::getNextChunkNr() const {
    return invoke(getNextChunkNumberProxy, this->getPipelineContext(), this->getOriginId(), this->getSequenceNumber());
}

void ExecutionContext::removeSequenceState() const {
    invoke(removeSequenceStateProxy, this->getPipelineContext(), this->getOriginId(), this->getSequenceNumber());
}

}// namespace NES::Runtime::Execution
