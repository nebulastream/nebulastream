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

#include <Join/NestedLoopJoin/NLJInnerProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
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

NLJInnerProbePhysicalOperator::NLJInnerProbePhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<TupleBufferRef> leftMemoryProvider,
    std::shared_ptr<TupleBufferRef> rightMemoryProvider,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames)
    : NLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftMemoryProvider),
          std::move(rightMemoryProvider),
          std::move(leftKeyFieldNames),
          std::move(rightKeyFieldNames))
{
}

void NLJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    /// Parse trigger buffer — for inner join, always 1 left + 1 right slice end
    const auto triggerRef = static_cast<nautilus::val<EmittedNLJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedNLJWindowTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    auto leftSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::leftSliceEnds));
    auto rightSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rightSliceEnds));
    const nautilus::val<SliceEnd> sliceIdLeft{leftSliceEndsPtr[0]};
    const nautilus::val<SliceEnd> sliceIdRight{rightSliceEndsPtr[0]};

    /// During triggering the slice, we append all pages of all local copies to a single PagedVector located at position 0
    const nautilus::val<WorkerThreadId> workerThreadIdForPages{WorkerThreadId(0)};

    /// Getting the left and right paged vector
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto sliceRefLeft = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdLeft);
    const auto sliceRefRight = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdRight);

    const auto leftPagedVectorRef = invoke(
        +[](const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
        {
            PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
            return nljSlice->getPagedVectorRef(workerThreadId, joinBuildSide);
        },
        sliceRefLeft,
        workerThreadIdForPages,
        nautilus::val<JoinBuildSideType>(JoinBuildSideType::Left));
    const auto rightPagedVectorRef = invoke(
        +[](const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
        {
            PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
            return nljSlice->getPagedVectorRef(workerThreadId, joinBuildSide);
        },
        sliceRefRight,
        workerThreadIdForPages,
        nautilus::val<JoinBuildSideType>(JoinBuildSideType::Right));

    const PagedVectorRef leftPagedVector(leftPagedVectorRef, leftMemoryProvider);
    const PagedVectorRef rightPagedVector(rightPagedVectorRef, rightMemoryProvider);
    const auto numberOfTuplesLeft = leftPagedVector.getNumberOfTuples();
    const auto numberOfTuplesRight = rightPagedVector.getNumberOfTuples();

    /// Outer loop should have more no. tuples
    if (numberOfTuplesLeft < numberOfTuplesRight)
    {
        performNLJ(
            leftPagedVector,
            rightPagedVector,
            *leftMemoryProvider,
            *rightMemoryProvider,
            leftKeyFieldNames,
            rightKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
    else
    {
        performNLJ(
            rightPagedVector,
            leftPagedVector,
            *rightMemoryProvider,
            *leftMemoryProvider,
            rightKeyFieldNames,
            leftKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
}

}
