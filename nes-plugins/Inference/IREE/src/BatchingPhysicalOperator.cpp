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
#include <memory>

#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <ExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <IREEInferenceOperatorHandler.hpp>
#include <BatchingPhysicalOperator.hpp>

namespace NES
{

void emitBatchesProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = dynamic_cast<IREEInferenceOperatorHandler*>(ptrOpHandler);
    const BufferMetaData bufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId);
    for (auto batchIt = opHandler->batches.begin(); batchIt != opHandler->batches.end(); ++batchIt)
    {
        auto batch = batchIt->get();
        batch->combinePagedVectors();
        opHandler->emitBatchesToProbe(*batch, bufferMetaData, pipelineCtx);
    }
}

BatchingPhysicalOperator::BatchingPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : WindowBuildPhysicalOperator(operatorHandlerId), memoryProvider(std::move(std::move(memoryProvider)))
{
}

void BatchingPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    const auto batchMemRef = nautilus::invoke(
        +[](OperatorHandler* ptrOpHandler)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            const auto* opHandler = dynamic_cast<IREEInferenceOperatorHandler*>(ptrOpHandler);
            return opHandler->getOrCreateNewBatch();
        }, operatorHandler);

    const auto batchPagedVectorMemRef = nautilus::invoke(
        +[](Batch* batch, const WorkerThreadId workerThreadId)
        {
            PRECONDITION(batch != nullptr, "batch context should not be null!");
            return batch->getPagedVectorRef(workerThreadId);
        },
        batchMemRef,
        executionCtx.workerThreadId);

    const Interface::PagedVectorRef batchPagedVectorRef(batchPagedVectorMemRef, memoryProvider);
    batchPagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);\
}

void BatchingPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer&) const
{
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    executionCtx.setLocalOperatorState(id, std::make_unique<WindowOperatorBuildLocalState>(operatorHandler));
}

void BatchingPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        emitBatchesProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);
}

}
