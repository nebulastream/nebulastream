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

#include <cstdint>
#include <memory>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Performs the NLJ probe for outer joins (left, right, full).
/// Handles MATCH_PAIRS tasks (via base class) and NULL_FILL tasks
/// (iterating preserved-side tuples, checking for matches via nested loop, emitting NULLs for unmatched).
class NLJOuterProbePhysicalOperator final : public NLJProbePhysicalOperatorBase
{
public:
    NLJOuterProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    static constexpr bool supportsJoinType(JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN || joinType == JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN
            || joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
    }

private:
    /// Null-fill probe: for each tuple on the preserved side, iterates all inner-side PagedVectors
    /// (across all inner slices) and evaluates the join predicate. Emits NULL-filled records only for
    /// tuples that have no match on the inner side.
    void performNullFillProbe(
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
        const nautilus::val<Timestamp>& windowEnd) const;
};
}
