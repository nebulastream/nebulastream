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
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

NLJOuterProbePhysicalOperator::NLJOuterProbePhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames)
    : NLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(leftKeyFieldNames),
          std::move(rightKeyFieldNames))
{
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) null-fill requires triple-nested iteration: preserved tuples × inner slices × inner tuples
void NLJOuterProbePhysicalOperator::performNullFillProbe(
    const PagedVectorRef& preservedPagedVector,
    PagedVectorTupleLayout& preservedTupleLayout,
    const std::vector<Record::RecordFieldIdentifier>& preservedKeyFieldNames,
    nautilus::val<SliceEnd::Underlying*> innerSliceEndsPtr,
    nautilus::val<uint64_t> innerNumberOfSliceEnds,
    JoinBuildSideType innerSide,
    const std::shared_ptr<PagedVectorTupleLayout>& innerTupleLayout,
    const std::vector<Record::RecordFieldIdentifier>& innerKeyFieldNames,
    const Schema<QualifiedUnboundField, Ordered>& nullSideSchema,
    const nautilus::val<OperatorHandler*>& operatorHandlerRef,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto preservedFields = getOrderedFieldNames(preservedTupleLayout.getSchema());

    for (auto preservedIt = preservedPagedVector.begin(); preservedIt != preservedPagedVector.end(); ++preservedIt)
    {
        /// Check all inner-side slices for a matching tuple
        nautilus::val<bool> matched(false);
        for (nautilus::val<uint64_t> innerSliceIdx = 0; innerSliceIdx < innerNumberOfSliceEnds; ++innerSliceIdx)
        {
            const nautilus::val<SliceEnd> innerSliceEnd{innerSliceEndsPtr[innerSliceIdx]};
            const auto innerPVRef = getPagedVectorBufferRef(operatorHandlerRef, innerSliceEnd, innerSide);
            const PagedVectorRef innerPagedVector(BorrowedNautilusBuffer::from(innerPVRef), innerTupleLayout);

            for (auto innerIt = innerPagedVector.begin(); innerIt != innerPagedVector.end(); ++innerIt)
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
            auto nullRecord = createNullFilledJoinedRecord(*preservedIt, windowStart, windowEnd, preservedFields, nullSideSchema);
            executeChild(executionCtx, nullRecord);
        }
    }
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) outer join dispatches by task type and handles multiple slices
void NLJOuterProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

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

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);

    if (probeTaskType == static_cast<uint64_t>(ProbeTaskType::LEFT_NULL_FILL))
    {
        /// Preserved side: left (1 slice). Inner side: right (all slices).
        if (leftNumberOfSliceEnds > 0)
        {
            const nautilus::val<SliceEnd> leftSliceEnd{leftSliceEndsPtr[0]};
            const auto leftPVRef = getPagedVectorBufferRef(operatorHandlerMemRef, leftSliceEnd, JoinBuildSideType::Left);
            const PagedVectorRef leftPagedVector(BorrowedNautilusBuffer::from(leftPVRef), leftTupleLayout);
            performNullFillProbe(
                leftPagedVector,
                *leftTupleLayout,
                leftKeyFieldNames,
                rightSliceEndsPtr,
                rightNumberOfSliceEnds,
                JoinBuildSideType::Right,
                rightTupleLayout,
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
            const auto rightPVRef = getPagedVectorBufferRef(operatorHandlerMemRef, rightSliceEnd, JoinBuildSideType::Right);
            const PagedVectorRef rightPagedVector(BorrowedNautilusBuffer::from(rightPVRef), rightTupleLayout);
            performNullFillProbe(
                rightPagedVector,
                *rightTupleLayout,
                rightKeyFieldNames,
                leftSliceEndsPtr,
                leftNumberOfSliceEnds,
                JoinBuildSideType::Left,
                leftTupleLayout,
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
    const auto leftPVRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdLeft, JoinBuildSideType::Left);
    const auto rightPVRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdRight, JoinBuildSideType::Right);

    const PagedVectorRef leftPagedVector(BorrowedNautilusBuffer::from(leftPVRef), leftTupleLayout);
    const PagedVectorRef rightPagedVector(BorrowedNautilusBuffer::from(rightPVRef), rightTupleLayout);
    const auto numberOfTuplesLeft = leftPagedVector.getNumberOfRecords();
    const auto numberOfTuplesRight = rightPagedVector.getNumberOfRecords();

    if (numberOfTuplesLeft < numberOfTuplesRight)
    {
        performNLJ(
            leftPagedVector,
            rightPagedVector,
            *leftTupleLayout,
            *rightTupleLayout,
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
            *rightTupleLayout,
            *leftTupleLayout,
            rightKeyFieldNames,
            leftKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
}

}
