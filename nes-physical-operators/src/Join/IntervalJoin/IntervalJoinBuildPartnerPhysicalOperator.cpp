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

#include <Join/IntervalJoin/IntervalJoinBuildPartnerPhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinBuildPhysicalOperator.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
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

/// setup-time proxy. Increments the partner store's pipeline counter so the handler knows how many
/// input pipelines will eventually terminate.
// todo this proxy function, asIntervalJoinHandler, and intervalJoinNotifyBufferDonePartnerProxy are quite similar. my idea would be to have a new enum values (anchor + partner) so that we can then pass these values via the constructor and have one operator handler method that expects a JoinBuildSide. Then in the operator handler, we do the different code calls.
void intervalJoinRegisterActivePipelinePartnerProxy(OperatorHandler* ptrOpHandler)
{
    asIntervalJoinHandler(ptrOpHandler)->getPartnerStore().incrementNumberOfInputPipelines();
}

/// Per-buffer close proxy. Advances the partner build watermark and re-runs the trigger loop.
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

/// Terminate proxy. Decrements the partner store's pipeline count; if zero, signals the handler that
/// the partner side is done. When both sides are done the handler runs the end-of-stream flush.
void intervalJoinTriggerAllPartnerProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = asIntervalJoinHandler(ptrOpHandler);
    if (handler->getPartnerStore().decrementAndCheckIfLastPipeline())
    {
        // todo we should not use JoinBuildSideType here as it will confuse readers of this code. either add a new enum value or get rid of it.
        handler->onSideBuildTerminated(JoinBuildSideType::Right, pipelineCtx);
    }
}

}

IntervalJoinBuildPartnerPhysicalOperator::IntervalJoinBuildPartnerPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<TupleBufferRef> bufferRef,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : IntervalJoinBuildPhysicalOperator{
        // todo we should not use JoinBuildSideType here as it will confuse readers of this code. either add a new enum value or get rid of it.
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
