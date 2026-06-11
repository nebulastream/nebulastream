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

#include <Join/IntervalJoin/IntervalJoinProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

SliceEnd getLeftSliceEndFromTriggerProxy(const EmittedIntervalJoinWindowTrigger* trigger)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    return trigger->leftSliceEnd;
}

SliceEnd getPartnerSliceEndProxy(const EmittedIntervalJoinWindowTrigger* trigger, const std::uint64_t partnerIndex)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    PRECONDITION(partnerIndex < EmittedIntervalJoinWindowTrigger::MAX_PARTNERS, "partnerIndex {} out of range", partnerIndex);
    return trigger->partnerSliceEnds[partnerIndex];
}

std::uint64_t getPartnerCountProxy(const EmittedIntervalJoinWindowTrigger* trigger)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    return static_cast<std::uint64_t>(trigger->partnerCount);
}

bool getRightNullFillPassProxy(const EmittedIntervalJoinWindowTrigger* trigger)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    return trigger->rightNullFillPass;
}

Timestamp getWindowStartFromTriggerProxy(const EmittedIntervalJoinWindowTrigger* trigger)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    return trigger->windowInfo.windowStart;
}

Timestamp getWindowEndFromTriggerProxy(const EmittedIntervalJoinWindowTrigger* trigger)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    return trigger->windowInfo.windowEnd;
}

IntervalJoinSlice* getLeftSliceByEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    const auto sliceOpt = handler->getLeftStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(sliceOpt.has_value(), "Left store has no slice for sliceEnd {}", sliceEnd);
    auto* ptr = dynamic_cast<IntervalJoinSlice*>(sliceOpt.value().get());
    INVARIANT(ptr != nullptr, "Left store should hold IntervalJoinSlice instances");
    return ptr;
}

IntervalJoinSlice* getRightSliceByEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    const auto sliceOpt = handler->getRightStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(sliceOpt.has_value(), "Right store has no slice for sliceEnd {}", sliceEnd);
    auto* ptr = dynamic_cast<IntervalJoinSlice*>(sliceOpt.value().get());
    INVARIANT(ptr != nullptr, "Right store should hold IntervalJoinSlice instances");
    return ptr;
}

PagedVector* getMergedPagedVectorProxy(const IntervalJoinSlice* slice)
{
    PRECONDITION(slice != nullptr, "slice should not be null");
    return slice->getMergedPagedVector();
}

void intervalJoinNotifyProbeBufferDoneProxy(
    OperatorHandler* ptrOpHandler,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    const BufferMetaData bufferMetaData{watermarkTs, SequenceData{sequenceNumber, chunkNumber, lastChunk}, originId};
    handler->notifyBufferDoneProbe(bufferMetaData);
}

void intervalJoinProbeStartProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    handler->start(*pipelineCtx, 0);
}

void intervalJoinProbeStopProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    handler->stop(QueryTerminationType::Graceful, *pipelineCtx);
}

}

IntervalJoinProbePhysicalOperator::IntervalJoinProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::unique_ptr<TimeFunction> leftTimeFunctionParam,
    std::unique_ptr<TimeFunction> rightTimeFunctionParam,
    const std::int64_t lowerBoundParam,
    const std::int64_t upperBoundParam,
    std::shared_ptr<TupleBufferRef> leftMemoryProviderParam,
    std::shared_ptr<TupleBufferRef> rightMemoryProviderParam,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNamesParam,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNamesParam,
    const bool emitLeftNullFillParam)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), WindowMetaData(std::move(windowMetaData)), joinSchema)
    , leftTimeFunction(std::move(leftTimeFunctionParam))
    , rightTimeFunction(std::move(rightTimeFunctionParam))
    , lowerBound(lowerBoundParam)
    , upperBound(upperBoundParam)
    , leftMemoryProvider(std::move(leftMemoryProviderParam))
    , rightMemoryProvider(std::move(rightMemoryProviderParam))
    , leftKeyFieldNames(std::move(leftKeyFieldNamesParam))
    , rightKeyFieldNames(std::move(rightKeyFieldNamesParam))
    , emitLeftNullFill(emitLeftNullFillParam)
{
}

IntervalJoinProbePhysicalOperator::IntervalJoinProbePhysicalOperator(const IntervalJoinProbePhysicalOperator& other)
    : StreamJoinProbePhysicalOperator(other)
    , leftTimeFunction(other.leftTimeFunction ? other.leftTimeFunction->clone() : nullptr)
    , rightTimeFunction(other.rightTimeFunction ? other.rightTimeFunction->clone() : nullptr)
    , lowerBound(other.lowerBound)
    , upperBound(other.upperBound)
    , leftMemoryProvider(other.leftMemoryProvider)
    , rightMemoryProvider(other.rightMemoryProvider)
    , leftKeyFieldNames(other.leftKeyFieldNames)
    , rightKeyFieldNames(other.rightKeyFieldNames)
    , emitLeftNullFill(other.emitLeftNullFill)
{
}

void IntervalJoinProbePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    invoke(intervalJoinProbeStartProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.pipelineContext);
}

void IntervalJoinProbePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    invoke(intervalJoinProbeStopProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.pipelineContext);
    terminateChild(executionCtx);
}

void IntervalJoinProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Propagate buffer metadata onto the execution context (same as NLJProbe).
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    /// Initialise the time functions (event-time / ingestion-time wiring).
    leftTimeFunction->open(executionCtx, recordBuffer);
    rightTimeFunction->open(executionCtx, recordBuffer);

    const auto triggerRef = static_cast<nautilus::val<EmittedIntervalJoinWindowTrigger*>>(recordBuffer.getMemArea());
    const auto anchorSliceEnd = invoke(getLeftSliceEndFromTriggerProxy, triggerRef);
    const auto windowStart = invoke(getWindowStartFromTriggerProxy, triggerRef);
    const auto windowEnd = invoke(getWindowEndFromTriggerProxy, triggerRef);
    const auto partnerCount = invoke(getPartnerCountProxy, triggerRef);
    const auto rightNullFillPass = invoke(getRightNullFillPassProxy, triggerRef);

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftFieldNames = leftMemoryProvider->getAllFieldNames();
    const auto rightFieldNames = rightMemoryProvider->getAllFieldNames();

    if (rightNullFillPass)
    {
        /// RIGHT/FULL outer null-fill pass: the anchor is a right slice and the partners are left slices.
        /// Matches were already emitted by the left-anchored pass, so here we only emit a null-filled row
        /// (left-side fields null) for each right tuple that matched no left tuple within the interval.
        const auto anchorSliceRef = invoke(getRightSliceByEndProxy, operatorHandlerMemRef, anchorSliceEnd);
        const auto anchorPagedVectorPtr = invoke(getMergedPagedVectorProxy, anchorSliceRef);
        const PagedVectorRef anchorPagedVector(anchorPagedVectorPtr, rightMemoryProvider);

        nautilus::val<std::uint64_t> outerPos{0};
        for (auto outerIt = anchorPagedVector.begin(rightKeyFieldNames); outerIt != anchorPagedVector.end(rightKeyFieldNames); ++outerIt)
        {
            nautilus::val<bool> matched{false};
            for (nautilus::val<std::uint64_t> partnerIdx{0}; partnerIdx < partnerCount;
                 partnerIdx = partnerIdx + nautilus::val<std::uint64_t>{1})
            {
                const auto partnerSliceEnd = invoke(getPartnerSliceEndProxy, triggerRef, partnerIdx);
                const auto leftSliceRef = invoke(getLeftSliceByEndProxy, operatorHandlerMemRef, partnerSliceEnd);
                const auto leftPagedVectorPtr = invoke(getMergedPagedVectorProxy, leftSliceRef);
                const PagedVectorRef partnerPagedVector(leftPagedVectorPtr, leftMemoryProvider);

                nautilus::val<std::uint64_t> innerPos{0};
                for (auto innerIt = partnerPagedVector.begin(leftKeyFieldNames); innerIt != partnerPagedVector.end(leftKeyFieldNames);
                     ++innerIt)
                {
                    /// joinFunction expects (left, right) field order; here outer = right, inner = left.
                    const auto candidateRecord
                        = createJoinedRecord(*innerIt, *outerIt, windowStart, windowEnd, leftKeyFieldNames, rightKeyFieldNames);
                    if (joinFunction.execute(candidateRecord, executionCtx.pipelineMemoryProvider.arena))
                    {
                        auto rightRecord = anchorPagedVector.readRecord(outerPos, rightFieldNames);
                        auto leftRecord = partnerPagedVector.readRecord(innerPos, leftFieldNames);
                        const auto leftTs = leftTimeFunction->getTs(executionCtx, leftRecord).convertToValue();
                        const auto rightTs = rightTimeFunction->getTs(executionCtx, rightRecord).convertToValue();

                        nautilus::val<bool> lowerOk{false};
                        if (lowerBound >= 0)
                        {
                            const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(lowerBound)};
                            lowerOk = (rightTs >= (leftTs + offset));
                        }
                        else
                        {
                            const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(-lowerBound)};
                            lowerOk = ((rightTs + offset) >= leftTs);
                        }

                        nautilus::val<bool> upperOk{false};
                        if (upperBound >= 0)
                        {
                            const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(upperBound)};
                            upperOk = (rightTs <= (leftTs + offset));
                        }
                        else
                        {
                            const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(-upperBound)};
                            upperOk = ((rightTs + offset) <= leftTs);
                        }

                        if (lowerOk && upperOk)
                        {
                            matched = nautilus::val<bool>{true};
                        }
                    }
                    innerPos = innerPos + nautilus::val<std::uint64_t>{1};
                }
            }
            if (!matched)
            {
                auto rightRecord = anchorPagedVector.readRecord(outerPos, rightFieldNames);
                auto nullRecord = createNullFilledJoinedRecord(rightRecord, windowStart, windowEnd, rightFieldNames, joinSchema.leftSchema);
                executeChild(executionCtx, nullRecord);
            }
            outerPos = outerPos + nautilus::val<std::uint64_t>{1};
        }
        return;
    }

    /// Anchor (left) slice: resolved once per buffer, then merged-page reused for every partner.
    const auto leftSliceRef = invoke(getLeftSliceByEndProxy, operatorHandlerMemRef, anchorSliceEnd);
    const auto leftPagedVectorPtr = invoke(getMergedPagedVectorProxy, leftSliceRef);
    const PagedVectorRef leftPagedVector(leftPagedVectorPtr, leftMemoryProvider);

    /// Outer loop over anchor (left) tuples so each left tuple is matched against every partner slice
    /// in a single pass. This lets a LEFT/FULL outer join detect an unmatched left tuple and null-fill
    /// it here: the trigger gathers every partner slice that can fall inside this anchor's interval
    /// range, so absence of a match across all partners means the left tuple truly has no partner.
    nautilus::val<std::uint64_t> outerPos{0};
    for (auto outerIt = leftPagedVector.begin(leftKeyFieldNames); outerIt != leftPagedVector.end(leftKeyFieldNames); ++outerIt)
    {
        nautilus::val<bool> matched{false};
        for (nautilus::val<std::uint64_t> partnerIdx{0}; partnerIdx < partnerCount;
             partnerIdx = partnerIdx + nautilus::val<std::uint64_t>{1})
        {
            const auto partnerSliceEnd = invoke(getPartnerSliceEndProxy, triggerRef, partnerIdx);
            const auto rightSliceRef = invoke(getRightSliceByEndProxy, operatorHandlerMemRef, partnerSliceEnd);
            const auto rightPagedVectorPtr = invoke(getMergedPagedVectorProxy, rightSliceRef);
            const PagedVectorRef rightPagedVector(rightPagedVectorPtr, rightMemoryProvider);

            nautilus::val<std::uint64_t> innerPos{0};
            for (auto innerIt = rightPagedVector.begin(rightKeyFieldNames); innerIt != rightPagedVector.end(rightKeyFieldNames); ++innerIt)
            {
                /// User join expression first (cheap join key compare); skip interval check on failure.
                const auto candidateRecord
                    = createJoinedRecord(*outerIt, *innerIt, windowStart, windowEnd, leftKeyFieldNames, rightKeyFieldNames);
                if (joinFunction.execute(candidateRecord, executionCtx.pipelineMemoryProvider.arena))
                {
                    auto outerRecord = leftPagedVector.readRecord(outerPos, leftFieldNames);
                    auto innerRecord = rightPagedVector.readRecord(innerPos, rightFieldNames);

                    /// Interval predicate: leftTs + lowerBound <= rightTs <= leftTs + upperBound.
                    /// Bounds are signed and resolved at C++ time, so we generate two distinct codegen
                    /// shapes (add or subtract) per side and keep the runtime check as plain unsigned compare.
                    const auto leftTs = leftTimeFunction->getTs(executionCtx, outerRecord).convertToValue();
                    const auto rightTs = rightTimeFunction->getTs(executionCtx, innerRecord).convertToValue();

                    nautilus::val<bool> lowerOk{false};
                    if (lowerBound >= 0)
                    {
                        const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(lowerBound)};
                        lowerOk = (rightTs >= (leftTs + offset));
                    }
                    else
                    {
                        const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(-lowerBound)};
                        lowerOk = ((rightTs + offset) >= leftTs);
                    }

                    nautilus::val<bool> upperOk{false};
                    if (upperBound >= 0)
                    {
                        const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(upperBound)};
                        upperOk = (rightTs <= (leftTs + offset));
                    }
                    else
                    {
                        const auto offset = nautilus::val<std::uint64_t>{static_cast<std::uint64_t>(-upperBound)};
                        upperOk = ((rightTs + offset) <= leftTs);
                    }

                    if (lowerOk && upperOk)
                    {
                        auto joinedRecord
                            = createJoinedRecord(outerRecord, innerRecord, windowStart, windowEnd, leftFieldNames, rightFieldNames);
                        executeChild(executionCtx, joinedRecord);
                        matched = nautilus::val<bool>{true};
                    }
                }
                innerPos = innerPos + nautilus::val<std::uint64_t>{1};
            }
        }
        /// LEFT/FULL outer: a left tuple with no partner across all partner slices emits one row with
        /// the right-side fields set to null. `emitLeftNullFill` is a compile-time flag, so inner joins
        /// generate none of this code.
        if (emitLeftNullFill)
        {
            if (!matched)
            {
                auto outerRecord = leftPagedVector.readRecord(outerPos, leftFieldNames);
                auto nullRecord = createNullFilledJoinedRecord(outerRecord, windowStart, windowEnd, leftFieldNames, joinSchema.rightSchema);
                executeChild(executionCtx, nullRecord);
            }
        }
        outerPos = outerPos + nautilus::val<std::uint64_t>{1};
    }
}

void IntervalJoinProbePhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        intervalJoinNotifyProbeBufferDoneProxy,
        operatorHandlerMemRef,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);

    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

}
