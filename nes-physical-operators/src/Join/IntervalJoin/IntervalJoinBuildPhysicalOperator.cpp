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

IntervalJoinOperatorHandler* asIntervalJoinHandler(OperatorHandler* ptrOpHandler)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    return handler;
}

/// setup-time proxies. Increment the side's slice-store pipeline counter so the handler
/// knows how many input pipelines will eventually terminate.
void intervalJoinRegisterActivePipelineAnchorProxy(OperatorHandler* ptrOpHandler)
{
    asIntervalJoinHandler(ptrOpHandler)->getAnchorStore().incrementNumberOfInputPipelines();
}

void intervalJoinRegisterActivePipelinePartnerProxy(OperatorHandler* ptrOpHandler)
{
    asIntervalJoinHandler(ptrOpHandler)->getPartnerStore().incrementNumberOfInputPipelines();
}

/// Per-buffer close proxies. Advance the side's build watermark and re-run the trigger loop.
void intervalJoinNotifyBufferDoneAnchorProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    const BufferMetaData bufferMetaData{watermarkTs, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId};
    asIntervalJoinHandler(ptrOpHandler)->notifyBufferDoneAnchor(bufferMetaData, pipelineCtx);
}

void intervalJoinNotifyBufferDonePartnerProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    const BufferMetaData bufferMetaData{watermarkTs, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId};
    asIntervalJoinHandler(ptrOpHandler)->notifyBufferDonePartner(bufferMetaData, pipelineCtx);
}

/// Terminate proxies. Decrement the side's pipeline count; if zero, signal the handler that
/// this side is done. When both sides are done the handler runs the end-of-stream flush.
void intervalJoinTriggerAllAnchorProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = asIntervalJoinHandler(ptrOpHandler);
    if (handler->getAnchorStore().decrementAndCheckIfLastPipeline())
    {
        handler->onSideBuildTerminated(JoinBuildSideType::Left, pipelineCtx);
    }
}

void intervalJoinTriggerAllPartnerProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = asIntervalJoinHandler(ptrOpHandler);
    if (handler->getPartnerStore().decrementAndCheckIfLastPipeline())
    {
        handler->onSideBuildTerminated(JoinBuildSideType::Right, pipelineCtx);
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

void IntervalJoinBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto pagedVectorMemRef = sliceStoreRef->getDataStructureRef(timestamp, executionCtx.workerThreadId, operatorHandler);

    const PagedVectorRef pagedVectorRef(pagedVectorMemRef, bufferRef);
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}

IntervalJoinBuildAnchorPhysicalOperator::IntervalJoinBuildAnchorPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<TupleBufferRef> bufferRef,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : IntervalJoinBuildPhysicalOperator{
          operatorHandlerId, JoinBuildSideType::Left, std::move(timeFunction), std::move(bufferRef), std::move(sliceStoreRef)}
{
}

void IntervalJoinBuildAnchorPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(intervalJoinRegisterActivePipelineAnchorProxy, operatorHandler);
    sliceStoreRef->setupSliceStore(executionCtx.pipelineContext);
}

void IntervalJoinBuildAnchorPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        intervalJoinNotifyBufferDoneAnchorProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);
}

void IntervalJoinBuildAnchorPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(intervalJoinTriggerAllAnchorProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}

IntervalJoinBuildPartnerPhysicalOperator::IntervalJoinBuildPartnerPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<TupleBufferRef> bufferRef,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : IntervalJoinBuildPhysicalOperator{
          operatorHandlerId, JoinBuildSideType::Right, std::move(timeFunction), std::move(bufferRef), std::move(sliceStoreRef)}
{
}

void IntervalJoinBuildPartnerPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(intervalJoinRegisterActivePipelinePartnerProxy, operatorHandler);
    sliceStoreRef->setupSliceStore(executionCtx.pipelineContext);
}

void IntervalJoinBuildPartnerPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        intervalJoinNotifyBufferDonePartnerProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);
}

void IntervalJoinBuildPartnerPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(intervalJoinTriggerAllPartnerProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}

}
