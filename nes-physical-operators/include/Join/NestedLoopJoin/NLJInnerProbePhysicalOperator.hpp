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
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Performs the NLJ probe for inner joins. Handles MATCH_PAIRS tasks, and for an unbounded window the MATCH_PAIRS_ITERATE_LEFT
/// and MATCH_PAIRS_ITERATE_RIGHT tasks, which carry their paged vectors as child buffers.
class NLJInnerProbePhysicalOperator final : public NLJProbePhysicalOperatorBase
{
public:
    NLJInnerProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames,
        bool unboundedWindow = false);

    static constexpr bool supportsJoinType(JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::INNER_JOIN || joinType == JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT;
    }

protected:
    void probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    void probeUnboundedWindow(ExecutionContext& executionCtx, const RecordBuffer& recordBuffer) const;

    /// An unbounded window emits other task buffers, so the probe traces the code for one of the two
    bool unboundedWindow;
};
}
