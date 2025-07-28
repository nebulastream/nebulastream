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
#include <BatchingPhysicalOperator.hpp>
#include <ExecutionContext.hpp>
#include <IREEBatchInferenceOperatorHandler.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

void emitBatchesProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = dynamic_cast<IREEBatchInferenceOperatorHandler*>(ptrOpHandler);
    ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;

    auto batchesToBeEmitted = opHandler->batches
        | std::views::filter([](const std::shared_ptr<Batch>& batch)
            {
                return batch && batch->state.load() == BatchState::MARKED_AS_CREATED;
            });
    auto batchesCount = static_cast<size_t>(std::ranges::distance(batchesToBeEmitted));

    for (const auto& batch : batchesToBeEmitted)
    {
        const bool lastChunk = chunkNumber == batchesCount;
        const SequenceData sequenceData{sequenceNumber, ChunkNumber(chunkNumber), lastChunk};

        opHandler->emitBatchesToProbe(*batch, sequenceData, pipelineCtx, watermarkTs);
        ++chunkNumber;
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
            const auto* opHandler = dynamic_cast<IREEBatchInferenceOperatorHandler*>(ptrOpHandler);
            return opHandler->getOrCreateNewBatch();
        }, operatorHandler);

    const auto batchPagedVectorMemRef = nautilus::invoke(
        +[](Batch* batch)
        {
            PRECONDITION(batch != nullptr, "batch context should not be null!");
            return batch->getPagedVectorRef();
        }, batchMemRef);

    const Interface::PagedVectorRef batchPagedVectorRef(batchPagedVectorMemRef, memoryProvider);
    batchPagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}

void BatchingPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer&) const
{
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    executionCtx.setLocalOperatorState(id, std::make_unique<WindowOperatorBuildLocalState>(operatorHandler));
}

void BatchingPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(
        emitBatchesProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber);
}

}
