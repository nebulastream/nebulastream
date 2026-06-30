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
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <val.hpp>

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

/// Maps the interval-join's anchor/partner labelling onto the shared StreamJoinBuildPhysicalOperator
/// base, which is parameterized on JoinBuildSideType. Anchor == Left and Partner == Right; the value is
/// only stored by the base and never read on the interval-join path.
constexpr JoinBuildSideType toJoinBuildSideType(const IntervalJoinBuildSide buildSide)
{
    return buildSide == IntervalJoinBuildSide::Anchor ? JoinBuildSideType::Left : JoinBuildSideType::Right;
}

/// Terminate proxy. Decrements the side's store pipeline count; if it was the last, signals the handler
/// that the side is done. When both sides are done the handler runs the end-of-stream flush.
void intervalJoinTriggerAllProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx, const IntervalJoinBuildSide side)
{
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = asIntervalJoinHandler(ptrOpHandler);
    if (handler->getStore(side).decrementAndCheckIfLastPipeline())
    {
        handler->onSideBuildTerminated(side, pipelineCtx);
    }
}

}

IntervalJoinBuildPhysicalOperator::IntervalJoinBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const IntervalJoinBuildSide buildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : StreamJoinBuildPhysicalOperator{operatorHandlerId, toJoinBuildSideType(buildSide), std::move(timeFunction), std::move(tupleLayout), std::move(sliceStoreRef)}
    , buildSide{buildSide}
{
}

void IntervalJoinBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    /// setup-time: increment the side's store pipeline counter so the handler knows how many input
    /// pipelines will eventually terminate.
    invoke(
        +[](OperatorHandler* ptrOpHandler, const IntervalJoinBuildSide side)
        { asIntervalJoinHandler(ptrOpHandler)->getStore(side).incrementNumberOfInputPipelines(); },
        operatorHandler,
        nautilus::val<IntervalJoinBuildSide>(buildSide));
    sliceStoreRef->setupSliceStore(executionCtx.pipelineContext);
}

void IntervalJoinBuildPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    /// Per-buffer close: advance the side's build watermark and re-run the trigger loop.
    invoke(
        +[](OperatorHandler* ptrOpHandler,
            PipelineExecutionContext* pipelineCtx,
            const Timestamp watermarkTs,
            const SequenceNumber sequenceNumber,
            const ChunkNumber chunkNumber,
            const bool lastChunk,
            const OriginId originId,
            const IntervalJoinBuildSide side)
        {
            PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
            const BufferMetaData bufferMetaData{watermarkTs, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId};
            asIntervalJoinHandler(ptrOpHandler)->notifyBufferDone(side, bufferMetaData, pipelineCtx);
        },
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId,
        nautilus::val<IntervalJoinBuildSide>(buildSide));
}

void IntervalJoinBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    const auto timestamp = timeFunction->getTs(executionCtx, record);
    auto pagedVectorMemRef = sliceStoreRef->getDataStructureRef(
        timestamp, executionCtx.workerThreadId, operatorHandler, executionCtx.pipelineMemoryProvider.bufferProvider);

    PagedVectorRef pagedVectorRef{BorrowedNautilusBuffer::from(pagedVectorMemRef), tupleLayout};
    pagedVectorRef.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}

void IntervalJoinBuildPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        intervalJoinTriggerAllProxy, operatorHandlerMemRef, executionCtx.pipelineContext, nautilus::val<IntervalJoinBuildSide>(buildSide));
}

}
