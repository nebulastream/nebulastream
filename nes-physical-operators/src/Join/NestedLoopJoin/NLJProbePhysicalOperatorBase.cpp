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
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/TimeBasedWindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
NLJSlice* getNLJSliceRefFromEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);
    return dynamic_cast<NLJSlice*>(slice.value().get());
}
}

NLJProbePhysicalOperatorBase::NLJProbePhysicalOperatorBase(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), WindowMetaData(std::move(windowMetaData)), joinSchema)
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftKeyFieldNames(std::move(leftKeyFieldNames))
    , rightKeyFieldNames(std::move(rightKeyFieldNames))
{
}

void NLJProbePhysicalOperatorBase::performNLJ(
    const PagedVectorRef& outerPagedVector,
    const PagedVectorRef& innerPagedVector,
    PagedVectorTupleLayout& outerTupleLayout,
    PagedVectorTupleLayout& innerTupleLayout,
    const std::vector<Record::RecordFieldIdentifier>& outerKeyFieldNames,
    const std::vector<Record::RecordFieldIdentifier>& innerKeyFieldNames,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto outerFields = getOrderedFieldNames(outerTupleLayout.getSchema());
    const auto innerFields = getOrderedFieldNames(innerTupleLayout.getSchema());

    for (auto outerIt = outerPagedVector.begin(); outerIt != outerPagedVector.end(); ++outerIt)
    {
        for (auto innerIt = innerPagedVector.begin(); innerIt != innerPagedVector.end(); ++innerIt)
        {
            const auto joinedKeyFields
                = createJoinedRecord(*outerIt, *innerIt, windowStart, windowEnd, outerKeyFieldNames, innerKeyFieldNames);
            if (joinFunction.execute(joinedKeyFields, executionCtx.pipelineMemoryProvider.arena))
            {
                auto joinedRecord = createJoinedRecord(*outerIt, *innerIt, windowStart, windowEnd, outerFields, innerFields);
                executeChild(executionCtx, joinedRecord);
            }
        }
    }
}

nautilus::val<const TupleBuffer*> NLJProbePhysicalOperatorBase::getPagedVectorBufferRef(
    const nautilus::val<OperatorHandler*>& operatorHandlerRef, const nautilus::val<SliceEnd>& sliceEnd, const JoinBuildSideType side) const
{
    /// During triggering, all pages of all local copies are appended to a single PagedVector at worker-thread 0.
    const nautilus::val<WorkerThreadId> workerThreadIdForPages{WorkerThreadId(0)};
    const auto sliceRef = invoke(getNLJSliceRefFromEndProxy, operatorHandlerRef, sliceEnd);
    return invoke(
        +[](const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
        {
            PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
            return nljSlice->getPagedVectorTupleBufferRef(workerThreadId, joinBuildSide);
        },
        sliceRef,
        workerThreadIdForPages,
        nautilus::val<JoinBuildSideType>(side));
}

}
