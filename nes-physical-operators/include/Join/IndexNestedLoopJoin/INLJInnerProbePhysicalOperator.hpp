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
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/IndexNestedLoopJoin/INLJJoinCondition.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>

namespace NES
{

class INLJInnerProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    INLJInnerProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<BTreeTupleLayout> leftTupleLayout,
        std::shared_ptr<BTreeTupleLayout> rightTupleLayout,
        INLJJoinCondition condition);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    static constexpr bool supportsJoinType(const JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::INNER_JOIN;
    }

private:
    std::shared_ptr<BTreeTupleLayout> leftTupleLayout;
    std::shared_ptr<BTreeTupleLayout> rightTupleLayout;
    INLJJoinCondition condition;
    std::vector<Record::RecordFieldIdentifier> leftFields;
    std::vector<Record::RecordFieldIdentifier> rightFields;
    mutable std::vector<std::shared_ptr<BTreeComparator>> comparators;
    mutable BTreeComparator* activeRightComparator = nullptr;
};

}
