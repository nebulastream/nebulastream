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

#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
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
#include <val_arith.hpp>

namespace NES
{

NLJProbePhysicalOperatorBase::NLJProbePhysicalOperatorBase(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<TupleBufferRef> leftMemoryProvider,
    std::shared_ptr<TupleBufferRef> rightMemoryProvider,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), WindowMetaData(std::move(windowMetaData)), joinSchema)
    , leftMemoryProvider(std::move(leftMemoryProvider))
    , rightMemoryProvider(std::move(rightMemoryProvider))
    , leftKeyFieldNames(std::move(leftKeyFieldNames))
    , rightKeyFieldNames(std::move(rightKeyFieldNames))
{
}

void NLJProbePhysicalOperatorBase::performNLJ(
    const PagedVectorRef& outerPagedVector,
    const PagedVectorRef& innerPagedVector,
    TupleBufferRef& outerMemoryProvider,
    TupleBufferRef& innerMemoryProvider,
    const std::vector<Record::RecordFieldIdentifier>& outerKeyFieldNames,
    const std::vector<Record::RecordFieldIdentifier>& innerKeyFieldNames,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto outerFields = outerMemoryProvider.getAllFieldNames();
    const auto innerFields = innerMemoryProvider.getAllFieldNames();

    nautilus::val<uint64_t> outerItemPos(0);
    for (auto outerIt = outerPagedVector.begin(outerKeyFieldNames); outerIt != outerPagedVector.end(outerKeyFieldNames); ++outerIt)
    {
        nautilus::val<uint64_t> innerItemPos(0);
        for (auto innerIt = innerPagedVector.begin(innerKeyFieldNames); innerIt != innerPagedVector.end(innerKeyFieldNames); ++innerIt)
        {
            const auto joinedKeyFields
                = createJoinedRecord(*outerIt, *innerIt, windowStart, windowEnd, outerKeyFieldNames, innerKeyFieldNames);
            if (joinFunction.execute(joinedKeyFields, executionCtx.pipelineMemoryProvider.arena))
            {
                auto outerRecord = outerPagedVector.readRecord(outerItemPos, outerFields);
                auto innerRecord = innerPagedVector.readRecord(innerItemPos, innerFields);
                auto joinedRecord = createJoinedRecord(outerRecord, innerRecord, windowStart, windowEnd, outerFields, innerFields);
                executeChild(executionCtx, joinedRecord);
            }

            innerItemPos = innerItemPos + nautilus::val<uint64_t>{1};
        }
        outerItemPos = outerItemPos + nautilus::val<uint64_t>{1};
    }
}

}
