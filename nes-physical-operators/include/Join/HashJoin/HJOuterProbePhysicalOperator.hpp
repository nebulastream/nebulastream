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
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/HashJoin/HJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <val_arith.hpp>

namespace NES
{

/// Performs the hash join probe for outer joins (left, right, full).
/// Handles MATCH_PAIRS tasks (via base class) and NULL_FILL tasks
/// (emitting unmatched tuples with NULL-filled fields for the missing side).
class HJOuterProbePhysicalOperator final : public HJProbePhysicalOperatorBase
{
public:
    HJOuterProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        JoinSchema joinSchema,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        HashMapOptions leftHashMapBasedOptions,
        HashMapOptions rightHashMapBasedOptions);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    static constexpr bool supportsJoinType(JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN || joinType == JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN
            || joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
    }

private:
    /// `outerOffset`/`innerOffset` are the absolute child-buffer indices (on the record buffer) at which the outer's
    /// resp. inner's hash map buffers start; left hash maps are stored before right ones.
    void performNullFillProbe(
        const nautilus::val<TupleBuffer*>& recordBufferRef,
        nautilus::val<uint64_t> outerOffset,
        nautilus::val<uint64_t> outerNumberOfHashMaps,
        nautilus::val<uint64_t> innerOffset,
        nautilus::val<uint64_t> innerNumberOfHashMaps,
        const HashMapOptions& outerHashMapOptions,
        const HashMapOptions& innerHashMapOptions,
        const std::shared_ptr<PagedVectorTupleLayout>& outerTupleLayout,
        const Schema<QualifiedUnboundField, Ordered>& nullSideSchema,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;
};

}
