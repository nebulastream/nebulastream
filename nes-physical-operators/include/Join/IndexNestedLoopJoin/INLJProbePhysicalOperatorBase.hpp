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
#include <Schema/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/IndexNestedLoopJoin/INLJJoinCondition.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

class INLJProbePhysicalOperatorBase : public StreamJoinProbePhysicalOperator
{
public:
    INLJProbePhysicalOperatorBase(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<BTreeTupleLayout> leftTupleLayout,
        std::shared_ptr<BTreeTupleLayout> rightTupleLayout,
        INLJJoinCondition condition,
        bool needsLeftComparator);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

protected:
    void performMatchPairsProbe(
        const nautilus::val<SliceEnd>& leftSliceEnd,
        const nautilus::val<SliceEnd>& rightSliceEnd,
        const nautilus::val<OperatorHandler*>& operatorHandler,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    void performNullFillProbe(
        const nautilus::val<SliceEnd>& preservedSliceEnd,
        JoinBuildSideType preservedSide,
        nautilus::val<SliceEnd::Underlying*> innerSliceEnds,
        nautilus::val<uint64_t> innerNumberOfSliceEnds,
        ExecutionContext& executionCtx,
        const nautilus::val<OperatorHandler*>& operatorHandler,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    /// Unbounded window: iterates the trees of one side and looks each record up in all trees of the other side. The trees are
    /// child buffers of the task buffer, see EmittedJoinStateTrigger. Looking up in the left trees requires the left comparator.
    void performUnboundedMatchPairsProbe(
        const BorrowedNautilusBuffer& trees,
        const nautilus::val<uint64_t>& leftNumberOfTrees,
        const nautilus::val<uint64_t>& rightNumberOfTrees,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd,
        bool iterateLeft) const;

    /// Unbounded window, COMPACT task: merges the left trees into one tree and the right trees into one tree and publishes both to
    /// the operator handler. Requires the left comparator.
    void performUnboundedCompaction(
        const BorrowedNautilusBuffer& trees,
        const nautilus::val<uint64_t>& leftNumberOfTrees,
        const nautilus::val<uint64_t>& rightNumberOfTrees,
        const nautilus::val<uint64_t>& segmentId,
        ExecutionContext& executionCtx) const;

    std::shared_ptr<BTreeTupleLayout> leftTupleLayout;
    std::shared_ptr<BTreeTupleLayout> rightTupleLayout;
    INLJJoinCondition condition;
    std::vector<Record::RecordFieldIdentifier> leftFields;
    std::vector<Record::RecordFieldIdentifier> rightFields;

private:
    OwnedNautilusBuffer mergeTrees(
        const BorrowedNautilusBuffer& trees,
        const nautilus::val<uint64_t>& firstChild,
        const nautilus::val<uint64_t>& numberOfTrees,
        const std::shared_ptr<BTreeTupleLayout>& tupleLayout,
        const BTreeComparator& comparator,
        ExecutionContext& executionCtx) const;

    mutable std::vector<std::shared_ptr<BTreeComparator>> comparators;
    mutable BTreeComparator* activeLeftComparator = nullptr;
    mutable BTreeComparator* activeRightComparator = nullptr;
    bool needsLeftComparator;
};

}
