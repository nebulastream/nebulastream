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
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/IntervalBound.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
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

const TupleBuffer* getMergedPagedVectorProxy(const IntervalJoinSlice* slice)
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
    const IntervalBound lowerBoundParam,
    const IntervalBound upperBoundParam,
    std::shared_ptr<PagedVectorTupleLayout> anchorTupleLayoutParam,
    std::shared_ptr<PagedVectorTupleLayout> partnerTupleLayoutParam,
    std::vector<Record::RecordFieldIdentifier> anchorKeyFieldNamesParam,
    std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNamesParam)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), WindowMetaData{std::move(windowMetaData)}, joinSchema)
    , anchorTimeFunction(std::move(anchorTimeFunctionParam))
    , partnerTimeFunction(std::move(partnerTimeFunctionParam))
    , lowerBound(lowerBoundParam)
    , upperBound(upperBoundParam)
    , anchorTupleLayout(std::move(anchorTupleLayoutParam))
    , partnerTupleLayout(std::move(partnerTupleLayoutParam))
    , anchorKeyFieldNames(std::move(anchorKeyFieldNamesParam))
    , partnerKeyFieldNames(std::move(partnerKeyFieldNamesParam))
{
}

IntervalJoinProbePhysicalOperator::IntervalJoinProbePhysicalOperator(const IntervalJoinProbePhysicalOperator& other)
    : StreamJoinProbePhysicalOperator(other)
    , anchorTimeFunction(other.anchorTimeFunction ? other.anchorTimeFunction->clone() : nullptr)
    , partnerTimeFunction(other.partnerTimeFunction ? other.partnerTimeFunction->clone() : nullptr)
    , lowerBound(other.lowerBound)
    , upperBound(other.upperBound)
    , anchorTupleLayout(other.anchorTupleLayout)
    , partnerTupleLayout(other.partnerTupleLayout)
    , anchorKeyFieldNames(other.anchorKeyFieldNames)
    , partnerKeyFieldNames(other.partnerKeyFieldNames)
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

    /// Deliberately the base concept, NOT the direct parent WindowProbePhysicalOperator::close: that runs window
    /// GC which dynamic_casts the handler to WindowBasedOperatorHandler (null for the interval handler -> SIGSEGV).
    /// NOLINTNEXTLINE(bugprone-parent-virtual-call)
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

void IntervalJoinProbePhysicalOperator::prepareOpen(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Propagate buffer metadata onto the execution context.
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    anchorTimeFunction->open(executionCtx, recordBuffer);
    partnerTimeFunction->open(executionCtx, recordBuffer);
}

void IntervalJoinProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    prepareOpen(executionCtx, recordBuffer);
    runJoinPass(executionCtx, recordBuffer);
}

nautilus::val<bool>
IntervalJoinProbePhysicalOperator::intervalPredicateHolds(ExecutionContext& executionCtx, Record& anchorRecord, Record& partnerRecord) const
{
    /// anchorTs + lowerBound <= partnerTs <= anchorTs + upperBound. Bounds are signed and resolved at C++ time,
    /// so each side traces either an add or a subtract, keeping the runtime check a plain unsigned compare.
    const auto anchorTs = anchorTimeFunction->getTs(executionCtx, anchorRecord).convertToValue();
    const auto partnerTs = partnerTimeFunction->getTs(executionCtx, partnerRecord).convertToValue();

    const auto lowerBoundMs = lowerBound.getRawValue();
    const auto upperBoundMs = upperBound.getRawValue();

    nautilus::val<bool> lowerOk{false};
    if (lowerBoundMs >= 0)
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(lowerBoundMs)};
        lowerOk = (partnerTs >= (anchorTs + offset));
    }
    else
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(-lowerBoundMs)};
        lowerOk = ((partnerTs + offset) >= anchorTs);
    }

    nautilus::val<bool> upperOk{false};
    if (upperBoundMs >= 0)
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(upperBoundMs)};
        upperOk = (partnerTs <= (anchorTs + offset));
    }
    else
    {
        const nautilus::val<std::uint64_t> offset{static_cast<std::uint64_t>(-upperBoundMs)};
        upperOk = ((partnerTs + offset) <= anchorTs);
    }

    return lowerOk && upperOk;
}

void IntervalJoinProbePhysicalOperator::runJoinPass(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto triggerRef = static_cast<nautilus::val<EmittedIntervalJoinWindowTrigger*>>(recordBuffer.getMemArea());
    const auto anchorSliceEnd = invoke(getAnchorSliceEndFromTriggerProxy, triggerRef);
    const auto windowStart = invoke(getWindowStartFromTriggerProxy, triggerRef);
    const auto windowEnd = invoke(getWindowEndFromTriggerProxy, triggerRef);
    const auto partnerCount = invoke(getPartnerCountProxy, triggerRef);

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto anchorFieldNames = getOrderedFieldNames(anchorTupleLayout->getSchema());
    const auto partnerFieldNames = getOrderedFieldNames(partnerTupleLayout->getSchema());

    /// Resolve the anchor slice once per buffer. Its merged page is reused for every partner.
    const auto anchorSliceRef = invoke(getAnchorSliceByEndProxy, operatorHandlerMemRef, anchorSliceEnd);
    const auto anchorPagedVectorPtr = invoke(getMergedPagedVectorProxy, anchorSliceRef);
    const PagedVectorRef anchorPagedVector{BorrowedNautilusBuffer::from(anchorPagedVectorPtr), anchorTupleLayout};

    /// Anchor-tuple outer loop: each anchor tuple is matched against every partner slice gathered for it.
    for (auto anchorIt = anchorPagedVector.begin(); anchorIt != anchorPagedVector.end(); ++anchorIt)
    {
        for (nautilus::val<std::uint64_t> partnerIdx{0}; partnerIdx < partnerCount;
             partnerIdx = partnerIdx + nautilus::val<std::uint64_t>{1})
        {
            const auto partnerSliceEnd = invoke(getPartnerSliceEndProxy, triggerRef, partnerIdx);
            const auto partnerSliceRef = invoke(getPartnerSliceByEndProxy, operatorHandlerMemRef, partnerSliceEnd);
            const auto partnerPagedVectorPtr = invoke(getMergedPagedVectorProxy, partnerSliceRef);
            const PagedVectorRef partnerPagedVector{BorrowedNautilusBuffer::from(partnerPagedVectorPtr), partnerTupleLayout};

            for (auto partnerIt = partnerPagedVector.begin(); partnerIt != partnerPagedVector.end(); ++partnerIt)
            {
                auto anchorRecord = *anchorIt;
                auto partnerRecord = *partnerIt;
                /// Check the join expression first (cheap key compare) and skip the interval check on failure.
                const auto candidateRecord
                    = createJoinedRecord(anchorRecord, partnerRecord, windowStart, windowEnd, anchorKeyFieldNames, partnerKeyFieldNames);
                if (joinFunction.execute(candidateRecord, executionCtx.pipelineMemoryProvider.arena))
                {
                    if (intervalPredicateHolds(executionCtx, anchorRecord, partnerRecord))
                    {
                        auto joinedRecord
                            = createJoinedRecord(anchorRecord, partnerRecord, windowStart, windowEnd, anchorFieldNames, partnerFieldNames);
                        executeChild(executionCtx, joinedRecord);
                    }
                }
            }
        }
    }
}

}
