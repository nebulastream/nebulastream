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
#include <Functions/PhysicalFunction.hpp>
#include <Join/HashJoin/HJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>

namespace NES
{

/// Performs the hash join probe for inner joins. Only handles MATCH_PAIRS tasks.
class HJInnerProbePhysicalOperator final : public HJProbePhysicalOperatorBase
{
public:
    HJInnerProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        JoinSchema joinSchema,
        std::shared_ptr<TupleBufferRef> leftBufferRef,
        std::shared_ptr<TupleBufferRef> rightBufferRef,
        HashMapOptions leftHashMapBasedOptions,
        HashMapOptions rightHashMapBasedOptions);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    static constexpr bool supportsJoinType(JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::INNER_JOIN || joinType == JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT;
    }
};

}
