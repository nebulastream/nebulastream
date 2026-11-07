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

#include <Join/IndexNestedLoopJoin/INLJInnerProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/IndexNestedLoopJoin/INLJOperatorHandler.hpp>
#include <Join/IndexNestedLoopJoin/INLJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{

INLJInnerProbePhysicalOperator::INLJInnerProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<BTreeTupleLayout> leftTupleLayout,
    std::shared_ptr<BTreeTupleLayout> rightTupleLayout,
    INLJJoinCondition condition,
    const bool unboundedWindow)
    : INLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(condition),
          /// An unbounded window also looks records up in the left trees and compacts them
          unboundedWindow)
    , unboundedWindow(unboundedWindow)
{
}

void INLJInnerProbePhysicalOperator::probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (unboundedWindow)
    {
        probeUnboundedWindow(executionCtx, recordBuffer);
        return;
    }

    const auto triggerRef = static_cast<nautilus::val<EmittedINLJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedINLJWindowTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};
    auto leftSliceEnds = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedINLJWindowTrigger::leftSliceEnds));
    auto rightSliceEnds = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedINLJWindowTrigger::rightSliceEnds));
    performMatchPairsProbe(
        nautilus::val<SliceEnd>{leftSliceEnds[0]},
        nautilus::val<SliceEnd>{rightSliceEnds[0]},
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        executionCtx,
        windowStart,
        windowEnd);
}

void INLJInnerProbePhysicalOperator::probeUnboundedWindow(ExecutionContext& executionCtx, const RecordBuffer& recordBuffer) const
{
    const auto triggerRef = static_cast<nautilus::val<EmittedJoinStateTrigger*>>(recordBuffer.getMemArea());
    const auto leftNumberOfTrees = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedJoinStateTrigger::leftNumberOfBuffers));
    const auto rightNumberOfTrees = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedJoinStateTrigger::rightNumberOfBuffers));
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedJoinStateTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};
    const nautilus::val<ProbeTaskType> probeTaskType
        = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedJoinStateTrigger::probeTaskType));
    const auto trees = BorrowedNautilusBuffer::from(recordBuffer.getReference());

    if (probeTaskType == ProbeTaskType::COMPACT)
    {
        const auto segmentId = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedJoinStateTrigger::segmentId));
        performUnboundedCompaction(trees, leftNumberOfTrees, rightNumberOfTrees, segmentId, executionCtx);
    }
    else if (probeTaskType == ProbeTaskType::MATCH_PAIRS_ITERATE_LEFT)
    {
        probeUnboundedIteratingLeft(executionCtx, trees, leftNumberOfTrees, rightNumberOfTrees, windowStart, windowEnd);
    }
    else
    {
        probeUnboundedIteratingRight(executionCtx, trees, leftNumberOfTrees, rightNumberOfTrees, windowStart, windowEnd);
    }
}

void INLJInnerProbePhysicalOperator::probeUnboundedIteratingLeft(
    ExecutionContext& executionCtx,
    const BorrowedNautilusBuffer& trees,
    const nautilus::val<uint64_t>& leftNumberOfTrees,
    const nautilus::val<uint64_t>& rightNumberOfTrees,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    performUnboundedMatchPairsProbe(trees, leftNumberOfTrees, rightNumberOfTrees, executionCtx, windowStart, windowEnd, true);
}

void INLJInnerProbePhysicalOperator::probeUnboundedIteratingRight(
    ExecutionContext& executionCtx,
    const BorrowedNautilusBuffer& trees,
    const nautilus::val<uint64_t>& leftNumberOfTrees,
    const nautilus::val<uint64_t>& rightNumberOfTrees,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    performUnboundedMatchPairsProbe(trees, leftNumberOfTrees, rightNumberOfTrees, executionCtx, windowStart, windowEnd, false);
}

}
