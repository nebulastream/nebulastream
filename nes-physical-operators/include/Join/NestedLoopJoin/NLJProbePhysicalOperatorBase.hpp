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
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Shared base for all NLJ probe operators (inner, outer).
/// Holds the NLJ-specific state (memory providers, key field names) and the match-pairs nested-loop logic.
/// This avoids duplicating the NLJ iteration across probe variants while keeping inner and outer
/// as independent siblings, neither inherits from the other.
class NLJProbePhysicalOperatorBase : public StreamJoinProbePhysicalOperator
{
public:
    NLJProbePhysicalOperatorBase(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<TupleBufferRef> leftMemoryProvider,
        std::shared_ptr<TupleBufferRef> rightMemoryProvider,
        std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames);

protected:
    /// Match-pairs nested-loop join: iterates outer × inner tuples, evaluates the join function on key fields,
    /// and emits joined records for matching pairs.
    void performNLJ(
        const PagedVectorRef& outerPagedVector,
        const PagedVectorRef& innerPagedVector,
        TupleBufferRef& outerMemoryProvider,
        TupleBufferRef& innerMemoryProvider,
        const std::vector<Record::RecordFieldIdentifier>& outerKeyFieldNames,
        const std::vector<Record::RecordFieldIdentifier>& innerKeyFieldNames,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    std::shared_ptr<TupleBufferRef> leftMemoryProvider;
    std::shared_ptr<TupleBufferRef> rightMemoryProvider;
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames;
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames;
};
}
