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

SliceEnd getAnchorSliceEndFromTriggerProxy(const EmittedIntervalJoinWindowTrigger* trigger)
{
    PRECONDITION(trigger != nullptr, "trigger should not be null");
    return trigger->anchorSliceEnd;
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

IntervalJoinSlice* getAnchorSliceByEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    const auto sliceOpt = handler->getAnchorStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(sliceOpt.has_value(), "Anchor store has no slice for sliceEnd {}", sliceEnd);
    auto* ptr = dynamic_cast<IntervalJoinSlice*>(sliceOpt.value().get());
    INVARIANT(ptr != nullptr, "Anchor store should hold IntervalJoinSlice instances");
    return ptr;
}

IntervalJoinSlice* getPartnerSliceByEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
    auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
    PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
    const auto sliceOpt = handler->getPartnerStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(sliceOpt.has_value(), "Partner store has no slice for sliceEnd {}", sliceEnd);
    auto* ptr = dynamic_cast<IntervalJoinSlice*>(sliceOpt.value().get());
    INVARIANT(ptr != nullptr, "Partner store should hold IntervalJoinSlice instances");
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

}

IntervalJoinProbePhysicalOperator::IntervalJoinProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::unique_ptr<TimeFunction> anchorTimeFunctionParam,
    std::unique_ptr<TimeFunction> partnerTimeFunctionParam,
    const std::int64_t lowerBoundParam,
    const std::int64_t upperBoundParam,
    std::shared_ptr<TupleBufferRef> anchorMemoryProviderParam,
    std::shared_ptr<TupleBufferRef> partnerMemoryProviderParam,
    std::vector<Record::RecordFieldIdentifier> anchorKeyFieldNamesParam,
    std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNamesParam,
    const bool emitAnchorNullFillParam)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), WindowMetaData{std::move(windowMetaData)}, joinSchema)
    , anchorTimeFunction(std::move(anchorTimeFunctionParam))
    , partnerTimeFunction(std::move(partnerTimeFunctionParam))
    , lowerBound(lowerBoundParam)
    , upperBound(upperBoundParam)
    , anchorMemoryProvider(std::move(anchorMemoryProviderParam))
    , partnerMemoryProvider(std::move(partnerMemoryProviderParam))
    , anchorKeyFieldNames(std::move(anchorKeyFieldNamesParam))
    , partnerKeyFieldNames(std::move(partnerKeyFieldNamesParam))
    , emitAnchorNullFill(emitAnchorNullFillParam)
{
}

IntervalJoinProbePhysicalOperator::IntervalJoinProbePhysicalOperator(const IntervalJoinProbePhysicalOperator& other)
    : StreamJoinProbePhysicalOperator(other)
    , anchorTimeFunction(other.anchorTimeFunction ? other.anchorTimeFunction->clone() : nullptr)
    , partnerTimeFunction(other.partnerTimeFunction ? other.partnerTimeFunction->clone() : nullptr)
    , lowerBound(other.lowerBound)
    , upperBound(other.upperBound)
    , anchorMemoryProvider(other.anchorMemoryProvider)
    , partnerMemoryProvider(other.partnerMemoryProvider)
    , anchorKeyFieldNames(other.anchorKeyFieldNames)
    , partnerKeyFieldNames(other.partnerKeyFieldNames)
    , emitAnchorNullFill(other.emitAnchorNullFill)
{
}

void IntervalJoinProbePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    invoke(
        +[](OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
            PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
            auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
            PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
            handler->start(*pipelineCtx, 0);
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        executionCtx.pipelineContext);
}

void IntervalJoinProbePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    invoke(
        +[](OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null");
            PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");
            auto* handler = dynamic_cast<IntervalJoinOperatorHandler*>(ptrOpHandler);
            PRECONDITION(handler != nullptr, "expected IntervalJoinOperatorHandler");
            handler->stop(QueryTerminationType::Graceful, *pipelineCtx);
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        executionCtx.pipelineContext);
    terminateChild(executionCtx);
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

void IntervalJoinProbePhysicalOperator::prepareOpen(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
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
    anchorTimeFunction->open(executionCtx, recordBuffer);
    partnerTimeFunction->open(executionCtx, recordBuffer);
}

nautilus::val<bool>
IntervalJoinProbePhysicalOperator::intervalPredicateHolds(ExecutionContext& executionCtx, Record& anchorRecord, Record& partnerRecord) const
{
    /// Interval predicate: anchorTs + lowerBound <= partnerTs <= anchorTs + upperBound.
    /// Bounds are signed and resolved at C++ time, so we generate two distinct codegen shapes
    /// (add or subtract) per side and keep the runtime check as plain unsigned compare.
    const auto anchorTs = anchorTimeFunction->getTs(executionCtx, anchorRecord).convertToValue();
    const auto partnerTs = partnerTimeFunction->getTs(executionCtx, partnerRecord).convertToValue();

    nautilus::val<bool> lowerOk{false};
    if (lowerBound >= 0)
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(lowerBound)};
        lowerOk = (partnerTs >= (anchorTs + offset));
    }
    else
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(-lowerBound)};
        lowerOk = ((partnerTs + offset) >= anchorTs);
    }

    nautilus::val<bool> upperOk{false};
    if (upperBound >= 0)
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(upperBound)};
        upperOk = (partnerTs <= (anchorTs + offset));
    }
    else
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(-upperBound)};
        upperOk = ((partnerTs + offset) <= anchorTs);
    }

    return lowerOk && upperOk;
}

// todo I do not like that we need to have a lot of ? operators for driverIsAnchor. Can we not get rid of them? Maybe do this in the callee location that we need to create a struct driver and passenger or something like that
void IntervalJoinProbePhysicalOperator::runJoinPass(
    ExecutionContext& executionCtx, RecordBuffer& recordBuffer, const bool driverIsAnchor) const
{
    const auto triggerRef = static_cast<nautilus::val<EmittedIntervalJoinWindowTrigger*>>(recordBuffer.getMemArea());
    const auto driverSliceEnd = invoke(getAnchorSliceEndFromTriggerProxy, triggerRef);
    const auto windowStart = invoke(getWindowStartFromTriggerProxy, triggerRef);
    const auto windowEnd = invoke(getWindowEndFromTriggerProxy, triggerRef);
    const auto partnerCount = invoke(getPartnerCountProxy, triggerRef);

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto anchorFieldNames = anchorMemoryProvider->getAllFieldNames();
    const auto partnerFieldNames = partnerMemoryProvider->getAllFieldNames();

    /// Role bindings, resolved at trace time from driverIsAnchor. Anchor-driven pass: driver is an anchor-store
    /// slice and the gathered partners are partner-store slices. Partner-null-fill pass: the roles swap.
    const auto& driverProvider = driverIsAnchor ? anchorMemoryProvider : partnerMemoryProvider;
    const auto& partnerSliceProvider = driverIsAnchor ? partnerMemoryProvider : anchorMemoryProvider;
    const auto& driverKeys = driverIsAnchor ? anchorKeyFieldNames : partnerKeyFieldNames;
    const auto& partnerSliceKeys = driverIsAnchor ? partnerKeyFieldNames : anchorKeyFieldNames;
    const auto& driverFields = driverIsAnchor ? anchorFieldNames : partnerFieldNames;
    const auto& partnerSliceFields = driverIsAnchor ? partnerFieldNames : anchorFieldNames;

    /// Driver slice: resolved once per buffer, then merged-page reused for every partner.
    const auto driverSliceRef = driverIsAnchor ? invoke(getAnchorSliceByEndProxy, operatorHandlerMemRef, driverSliceEnd)
                                               : invoke(getPartnerSliceByEndProxy, operatorHandlerMemRef, driverSliceEnd);
    const auto driverPagedVectorPtr = invoke(getMergedPagedVectorProxy, driverSliceRef);
    const PagedVectorRef driverPagedVector{driverPagedVectorPtr, driverProvider};

    /// Outer loop over driver tuples so each is matched against every partner slice in a single pass. This lets
    /// an unmatched driver tuple be detected and null-filled here: the trigger gathers every partner slice that
    /// can fall inside this driver's interval range, so absence of a match across all partners means no partner.
    nautilus::val<std::uint64_t> outerPos{0};
    for (auto outerIt = driverPagedVector.begin(driverKeys); outerIt != driverPagedVector.end(driverKeys); ++outerIt)
    {
        nautilus::val<bool> matched{false};
        for (nautilus::val<std::uint64_t> partnerIdx{0}; partnerIdx < partnerCount;
             partnerIdx = partnerIdx + nautilus::val<std::uint64_t>{1})
        {
            const auto partnerSliceEnd = invoke(getPartnerSliceEndProxy, triggerRef, partnerIdx);
            const auto partnerSliceRef = driverIsAnchor ? invoke(getPartnerSliceByEndProxy, operatorHandlerMemRef, partnerSliceEnd)
                                                        : invoke(getAnchorSliceByEndProxy, operatorHandlerMemRef, partnerSliceEnd);
            const auto partnerPagedVectorPtr = invoke(getMergedPagedVectorProxy, partnerSliceRef);
            const PagedVectorRef partnerPagedVector{partnerPagedVectorPtr, partnerSliceProvider};

            nautilus::val<std::uint64_t> innerPos{0};
            for (auto innerIt = partnerPagedVector.begin(partnerSliceKeys); innerIt != partnerPagedVector.end(partnerSliceKeys); ++innerIt)
            {
                /// joinFunction expects (anchor, partner) field order. The anchor-side tuple is the driver in the
                /// anchor-driven pass and the partner-slice tuple in the null-fill pass. User join expression first
                /// (cheap join key compare); skip the interval check on failure.
                const auto candidateRecord = driverIsAnchor
                    ? createJoinedRecord(*outerIt, *innerIt, windowStart, windowEnd, anchorKeyFieldNames, partnerKeyFieldNames)
                    : createJoinedRecord(*innerIt, *outerIt, windowStart, windowEnd, anchorKeyFieldNames, partnerKeyFieldNames);
                if (joinFunction.execute(candidateRecord, executionCtx.pipelineMemoryProvider.arena))
                {
                    auto driverRecord = driverPagedVector.readRecord(outerPos, driverFields);
                    auto partnerSliceRecord = partnerPagedVector.readRecord(innerPos, partnerSliceFields);
                    auto& anchorRecord = driverIsAnchor ? driverRecord : partnerSliceRecord;
                    auto& partnerRecord = driverIsAnchor ? partnerSliceRecord : driverRecord;
                    if (intervalPredicateHolds(executionCtx, anchorRecord, partnerRecord))
                    {
                        /// The anchor-driven pass emits matched rows; the partner-null-fill pass does not
                        /// (those matches were already emitted), it only records that a partner matched.
                        if (driverIsAnchor)
                        {
                            auto joinedRecord = createJoinedRecord(
                                anchorRecord, partnerRecord, windowStart, windowEnd, anchorFieldNames, partnerFieldNames);
                            executeChild(executionCtx, joinedRecord);
                        }
                        matched = nautilus::val<bool>{true};
                    }
                }
                innerPos = innerPos + nautilus::val<std::uint64_t>{1};
            }
        }
        /// Null-fill the unmatched driver tuple: anchor-driven pass only for LEFT/FULL (emitAnchorNullFill,
        /// partner-side null); the partner-null-fill pass always (anchor-side null). Both flags are compile-time,
        /// so the inner probe generates none of this code.
        const bool doNullFill = driverIsAnchor ? emitAnchorNullFill : true;
        if (doNullFill)
        {
            if (!matched)
            {
                auto driverRecord = driverPagedVector.readRecord(outerPos, driverFields);
                const auto& nullSchema = driverIsAnchor ? joinSchema.rightSchema : joinSchema.leftSchema;
                auto nullRecord = createNullFilledJoinedRecord(driverRecord, windowStart, windowEnd, driverFields, nullSchema);
                executeChild(executionCtx, nullRecord);
            }
        }
        outerPos = outerPos + nautilus::val<std::uint64_t>{1};
    }
}

nautilus::val<bool> IntervalJoinProbePhysicalOperator::isPartnerNullFillPass(RecordBuffer& recordBuffer) const
{
    const auto triggerRef = static_cast<nautilus::val<EmittedIntervalJoinWindowTrigger*>>(recordBuffer.getMemArea());
    return invoke(
        +[](const EmittedIntervalJoinWindowTrigger* trigger)
        {
            PRECONDITION(trigger != nullptr, "trigger should not be null");
            return trigger->partnerNullFillPass;
        },
        triggerRef);
}

}
