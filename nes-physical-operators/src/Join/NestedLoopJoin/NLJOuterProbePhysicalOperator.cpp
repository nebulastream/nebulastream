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

#include <Join/NestedLoopJoin/NLJOuterProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
NLJSlice* getNLJSliceRefFromEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);
    return dynamic_cast<NLJSlice*>(slice.value().get());
}
}

NLJOuterProbePhysicalOperator::NLJOuterProbePhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<TupleBufferRef> leftMemoryProvider,
    std::shared_ptr<TupleBufferRef> rightMemoryProvider,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames)
    : NLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftMemoryProvider),
          std::move(rightMemoryProvider),
          std::move(leftKeyFieldNames),
          std::move(rightKeyFieldNames))
{
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) null-fill requires triple-nested iteration: preserved tuples × inner slices × inner tuples
void NLJOuterProbePhysicalOperator::performNullFillProbe(
    const PagedVectorRef& preservedPagedVector,
    TupleBufferRef& preservedMemoryProvider,
    const std::vector<Record::RecordFieldIdentifier>& preservedKeyFieldNames,
    nautilus::val<SliceEnd::Underlying*> innerSliceEndsPtr,
    nautilus::val<uint64_t> innerNumberOfSliceEnds,
    JoinBuildSideType innerSide,
    const std::shared_ptr<TupleBufferRef>& innerMemoryProvider,
    const std::vector<Record::RecordFieldIdentifier>& innerKeyFieldNames,
    const Schema& nullSideSchema,
    const nautilus::val<OperatorHandler*>& operatorHandlerRef,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto preservedFields = preservedMemoryProvider.getAllFieldNames();
    const auto workerThreadIdForPages = nautilus::val<WorkerThreadId>(WorkerThreadId(0));

    nautilus::val<uint64_t> preservedItemPos(0);
    for (auto preservedIt = preservedPagedVector.begin(preservedKeyFieldNames);
         preservedIt != preservedPagedVector.end(preservedKeyFieldNames);
         ++preservedIt)
    {
        /// Check all inner-side slices for a matching tuple
        nautilus::val<bool> matched(false);
        for (nautilus::val<uint64_t> innerSliceIdx = 0; innerSliceIdx < innerNumberOfSliceEnds; ++innerSliceIdx)
        {
            const nautilus::val<SliceEnd> innerSliceEnd{innerSliceEndsPtr[innerSliceIdx]};
            const auto innerSliceRef = invoke(getNLJSliceRefFromEndProxy, operatorHandlerRef, innerSliceEnd);
            const auto innerPVRef = invoke(
                +[](const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
                {
                    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
                    return nljSlice->getPagedVectorRef(workerThreadId, joinBuildSide);
                },
                innerSliceRef,
                workerThreadIdForPages,
                nautilus::val<JoinBuildSideType>(innerSide));
            const PagedVectorRef innerPagedVector(innerPVRef, innerMemoryProvider);

            for (auto innerIt = innerPagedVector.begin(innerKeyFieldNames); innerIt != innerPagedVector.end(innerKeyFieldNames); ++innerIt)
            {
                const auto joinedKeyFields
                    = createJoinedRecord(*preservedIt, *innerIt, windowStart, windowEnd, preservedKeyFieldNames, innerKeyFieldNames);
                if (joinFunction.execute(joinedKeyFields, executionCtx.pipelineMemoryProvider.arena))
                {
                    matched = nautilus::val<bool>(true);
                    break;
                }
            }

            if (matched)
            {
                break;
            }
        }

        if (!matched)
        {
            auto preservedRecord = preservedPagedVector.readRecord(preservedItemPos, preservedFields);
            auto nullRecord = createNullFilledJoinedRecord(preservedRecord, windowStart, windowEnd, preservedFields, nullSideSchema);
            executeChild(executionCtx, nullRecord);
        }

        preservedItemPos = preservedItemPos + nautilus::val<uint64_t>{1};
    }
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) outer join dispatches by task type and handles multiple slices
void NLJOuterProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    /// Parse trigger buffer
    const auto triggerRef = static_cast<nautilus::val<EmittedNLJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedNLJWindowTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    const auto leftNumberOfSliceEnds
        = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::leftNumberOfSliceEnds));
    const auto rightNumberOfSliceEnds
        = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rightNumberOfSliceEnds));
    auto leftSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::leftSliceEnds));
    auto rightSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rightSliceEnds));
    const auto probeTaskType = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::probeTaskType));

    const auto workerThreadIdForPages = nautilus::val<WorkerThreadId>(WorkerThreadId(0));
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);

    auto getPagedVectorFromSliceEnd = [&](const nautilus::val<SliceEnd>& sliceEnd, const JoinBuildSideType side)
    {
        const auto sliceRef = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceEnd);
        return invoke(
            +[](const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
            {
                PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
                return nljSlice->getPagedVectorRef(workerThreadId, joinBuildSide);
            },
            sliceRef,
            workerThreadIdForPages,
            nautilus::val<JoinBuildSideType>(side));
    };

    if (probeTaskType == static_cast<uint64_t>(ProbeTaskType::LEFT_NULL_FILL))
    {
        /// Preserved side: left (1 slice). Inner side: right (all slices).
        if (leftNumberOfSliceEnds > 0)
        {
            const nautilus::val<SliceEnd> leftSliceEnd{leftSliceEndsPtr[0]};
            const auto leftPVRef = getPagedVectorFromSliceEnd(leftSliceEnd, JoinBuildSideType::Left);
            const PagedVectorRef leftPagedVector(leftPVRef, leftMemoryProvider);
            performNullFillProbe(
                leftPagedVector,
                *leftMemoryProvider,
                leftKeyFieldNames,
                rightSliceEndsPtr,
                rightNumberOfSliceEnds,
                JoinBuildSideType::Right,
                rightMemoryProvider,
                rightKeyFieldNames,
                joinSchema.rightSchema,
                operatorHandlerMemRef,
                executionCtx,
                windowStart,
                windowEnd);
        }
        return;
    }

    if (probeTaskType == static_cast<uint64_t>(ProbeTaskType::RIGHT_NULL_FILL))
    {
        /// Preserved side: right (1 slice). Inner side: left (all slices).
        if (rightNumberOfSliceEnds > 0)
        {
            const nautilus::val<SliceEnd> rightSliceEnd{rightSliceEndsPtr[0]};
            const auto rightPVRef = getPagedVectorFromSliceEnd(rightSliceEnd, JoinBuildSideType::Right);
            const PagedVectorRef rightPagedVector(rightPVRef, rightMemoryProvider);
            performNullFillProbe(
                rightPagedVector,
                *rightMemoryProvider,
                rightKeyFieldNames,
                leftSliceEndsPtr,
                leftNumberOfSliceEnds,
                JoinBuildSideType::Left,
                leftMemoryProvider,
                leftKeyFieldNames,
                joinSchema.leftSchema,
                operatorHandlerMemRef,
                executionCtx,
                windowStart,
                windowEnd);
        }
        return;
    }

    /// MATCH_PAIRS: same as inner join — 1 left + 1 right slice
    const nautilus::val<SliceEnd> sliceIdLeft{leftSliceEndsPtr[nautilus::val<uint64_t>{0}]};
    const nautilus::val<SliceEnd> sliceIdRight{rightSliceEndsPtr[nautilus::val<uint64_t>{0}]};
    const auto leftPVRef = getPagedVectorFromSliceEnd(sliceIdLeft, JoinBuildSideType::Left);
    const auto rightPVRef = getPagedVectorFromSliceEnd(sliceIdRight, JoinBuildSideType::Right);

    const PagedVectorRef leftPagedVector(leftPVRef, leftMemoryProvider);
    const PagedVectorRef rightPagedVector(rightPVRef, rightMemoryProvider);
    const auto numberOfTuplesLeft = leftPagedVector.getNumberOfTuples();
    const auto numberOfTuplesRight = rightPagedVector.getNumberOfTuples();

    if (numberOfTuplesLeft < numberOfTuplesRight)
    {
        performNLJ(
            leftPagedVector,
            rightPagedVector,
            *leftMemoryProvider,
            *rightMemoryProvider,
            leftKeyFieldNames,
            rightKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
    else
    {
        performNLJ(
            rightPagedVector,
            leftPagedVector,
            *rightMemoryProvider,
            *leftMemoryProvider,
            rightKeyFieldNames,
            leftKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
}

}
