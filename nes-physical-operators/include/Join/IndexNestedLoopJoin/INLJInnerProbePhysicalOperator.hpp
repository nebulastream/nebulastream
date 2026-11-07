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

#pragma once

#include <memory>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/IndexNestedLoopJoin/INLJJoinCondition.hpp>
#include <Join/IndexNestedLoopJoin/INLJProbePhysicalOperatorBase.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>

namespace NES
{

class INLJInnerProbePhysicalOperator final : public INLJProbePhysicalOperatorBase
{
public:
    INLJInnerProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<BTreeTupleLayout> leftTupleLayout,
        std::shared_ptr<BTreeTupleLayout> rightTupleLayout,
        INLJJoinCondition condition,
        bool unboundedWindow = false);

    static constexpr bool supportsJoinType(const JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::INNER_JOIN;
    }

protected:
    void probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    void probeUnboundedWindow(ExecutionContext& executionCtx, const RecordBuffer& recordBuffer) const;

    /// Each task type of an unbounded window gets its own function. Calling one shared function with different arguments from the
    /// arms of the traced branch on the task type would let the host compiler merge the calls, and nautilus would then trace both
    /// arms as one in compiled mode.
    [[gnu::noinline]] void probeUnboundedIteratingLeft(
        ExecutionContext& executionCtx,
        const BorrowedNautilusBuffer& trees,
        const nautilus::val<uint64_t>& leftNumberOfTrees,
        const nautilus::val<uint64_t>& rightNumberOfTrees,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;
    [[gnu::noinline]] void probeUnboundedIteratingRight(
        ExecutionContext& executionCtx,
        const BorrowedNautilusBuffer& trees,
        const nautilus::val<uint64_t>& leftNumberOfTrees,
        const nautilus::val<uint64_t>& rightNumberOfTrees,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    /// An unbounded window emits other task buffers, so the probe traces the code for one of the two
    bool unboundedWindow;
};

}
