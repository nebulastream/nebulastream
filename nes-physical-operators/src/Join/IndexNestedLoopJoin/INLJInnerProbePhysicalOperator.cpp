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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
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
    INLJJoinCondition condition)
    : INLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(condition),
          false)
{
}

void INLJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);
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

}
