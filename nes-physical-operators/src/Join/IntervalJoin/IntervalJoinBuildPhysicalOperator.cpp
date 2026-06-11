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

#include <Join/IntervalJoin/IntervalJoinBuildPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>

namespace NES
{

namespace
{

/// setup-time proxy. Increments the side's slice-store pipeline counter so
/// the handler knows how many input pipelines will eventually terminate.
void intervalJoinRegisterActivePipelineProxy(OperatorHandler* ptrOpHandler, const std::uint8_t side)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    auto& store = (static_cast<JoinBuildSideType>(side) == JoinBuildSideType::Left) ? handler->getLeftStore() : handler->getRightStore();
    store.incrementNumberOfInputPipelines();
}

/// Per-buffer close proxy. Routes to the side-appropriate notifyBufferDone*.
void intervalJoinNotifyBufferDoneProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId,
    const std::uint8_t side)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");

    const BufferMetaData bufferMetaData{watermarkTs, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId};
    if (static_cast<JoinBuildSideType>(side) == JoinBuildSideType::Left)
    {
        handler->notifyBufferDoneLeft(bufferMetaData, pipelineCtx);
    }
    else
    {
        handler->notifyBufferDoneRight(bufferMetaData, pipelineCtx);
    }
}

/// Terminate proxy. Decrements the side's pipeline count; if zero, signals
/// the handler that this side is done. When both sides are done the handler
/// runs the end-of-stream flush.
void intervalJoinTriggerAllProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx, const std::uint8_t side)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");

    auto& store = (static_cast<JoinBuildSideType>(side) == JoinBuildSideType::Left) ? handler->getLeftStore() : handler->getRightStore();
    if (store.decrementAndCheckIfLastPipeline())
    {
        handler->onSideBuildTerminated(static_cast<JoinBuildSideType>(side), pipelineCtx);
    }
}

}

IntervalJoinBuildPhysicalOperator::IntervalJoinBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<TupleBufferRef> bufferRef,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : StreamJoinBuildPhysicalOperator{
          operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(bufferRef), std::move(sliceStoreRef)}
{
}

void IntervalJoinBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(intervalJoinRegisterActivePipelineProxy, operatorHandler, nautilus::val<std::uint8_t>{static_cast<std::uint8_t>(joinBuildSide)});
    sliceStoreRef->setupSliceStore(executionCtx.pipelineContext);
}

void IntervalJoinBuildPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        intervalJoinNotifyBufferDoneProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId,
        nautilus::val<std::uint8_t>{static_cast<std::uint8_t>(joinBuildSide)});
}

void IntervalJoinBuildPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        intervalJoinTriggerAllProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        nautilus::val<std::uint8_t>{static_cast<std::uint8_t>(joinBuildSide)});
}

void IntervalJoinBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto pagedVectorMemRef = sliceStoreRef->getDataStructureRef(timestamp, executionCtx.workerThreadId, operatorHandler);

    const PagedVectorRef pagedVectorRef(pagedVectorMemRef, bufferRef);
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}

}
