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

#include <cstdint>
#include <memory>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

NLJSlice* getNLJSliceRefFromEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);

    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);

    return dynamic_cast<NLJSlice*>(slice.value().get());
}

Timestamp getNLJWindowStartProxy(const EmittedNLJWindowTrigger* nljWindowTriggerTask)
{
    PRECONDITION(nljWindowTriggerTask, "nljWindowTriggerTask should not be null");
    return nljWindowTriggerTask->windowInfo.windowStart;
}

Timestamp getNLJWindowEndProxy(const EmittedNLJWindowTrigger* nljWindowTriggerTask)
{
    PRECONDITION(nljWindowTriggerTask, "nljWindowTriggerTask should not be null");
    return nljWindowTriggerTask->windowInfo.windowEnd;
}

static SliceEnd getNLJSliceEndProxy(const EmittedNLJWindowTrigger* nljWindowTriggerTask, const JoinBuildSideType joinBuildSide)
{
    PRECONDITION(nljWindowTriggerTask != nullptr, "nljWindowTriggerTask should not be null");

    switch (joinBuildSide)
    {
        case JoinBuildSideType::Left:
            return nljWindowTriggerTask->leftSliceEnd;
        case JoinBuildSideType::Right:
            return nljWindowTriggerTask->rightSliceEnd;
    }
}

NLJProbePhysicalOperator::NLJProbePhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<TupleBufferMemoryProvider> leftMemoryProvider,
    std::shared_ptr<TupleBufferMemoryProvider> rightMemoryProvider)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), WindowMetaData(std::move(windowMetaData)), joinSchema)
    , leftMemoryProvider(std::move(leftMemoryProvider))
    , rightMemoryProvider(std::move(rightMemoryProvider))
{
}

void NLJProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    /// Getting all needed info from the recordBuffer
    const auto nljWindowTriggerTaskRef = static_cast<nautilus::val<EmittedNLJWindowTrigger*>>(recordBuffer.getBuffer());
    const auto sliceIdLeft
        = invoke(getNLJSliceEndProxy, nljWindowTriggerTaskRef, nautilus::val<JoinBuildSideType>(JoinBuildSideType::Left));
    const auto sliceIdRight
        = invoke(getNLJSliceEndProxy, nljWindowTriggerTaskRef, nautilus::val<JoinBuildSideType>(JoinBuildSideType::Right));
    const auto windowStart = invoke(getNLJWindowStartProxy, nljWindowTriggerTaskRef);
    const auto windowEnd = invoke(getNLJWindowEndProxy, nljWindowTriggerTaskRef);

    /// During triggering the slice, we append all pages of all local copies to a single PagedVector located at position 0
    const auto workerThreadIdForPages = nautilus::val<WorkerThreadId>(WorkerThreadId(0));

    /// Getting the left and right paged vector
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto sliceRefLeft = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdLeft);
    const auto sliceRefRight = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdRight);

    const auto leftPagedVectorRef
        = invoke(getNLJPagedVectorProxy, sliceRefLeft, workerThreadIdForPages, nautilus::val<JoinBuildSideType>(JoinBuildSideType::Left));
    const auto rightPagedVectorRef
        = invoke(getNLJPagedVectorProxy, sliceRefRight, workerThreadIdForPages, nautilus::val<JoinBuildSideType>(JoinBuildSideType::Right));

    const Interface::PagedVectorRef leftPagedVector(
        leftPagedVectorRef, leftMemoryProvider, executionCtx.pipelineMemoryProvider.bufferProvider);
    const Interface::PagedVectorRef rightPagedVector(
        rightPagedVectorRef, rightMemoryProvider, executionCtx.pipelineMemoryProvider.bufferProvider);

    const auto leftKeyFields = leftMemoryProvider->getMemoryLayout()->getKeyFieldNames();
    const auto rightKeyFields = rightMemoryProvider->getMemoryLayout()->getKeyFieldNames();
    const auto leftFields = leftMemoryProvider->getMemoryLayout()->getSchema().getFieldNames();
    const auto rightFields = rightMemoryProvider->getMemoryLayout()->getSchema().getFieldNames();

    nautilus::val<uint64_t> leftItemPos = 0UL;
    for (auto leftIt = leftPagedVector.begin(leftKeyFields); leftIt != leftPagedVector.end(leftKeyFields); ++leftIt)
    {
        nautilus::val<uint64_t> rightItemPos = 0UL;
        for (auto rightIt = rightPagedVector.begin(rightKeyFields); rightIt != rightPagedVector.end(rightKeyFields); ++rightIt)
        {
            auto joinedKeyFields = createJoinedRecord(*leftIt, *rightIt, windowStart, windowEnd, leftKeyFields, rightKeyFields);
            if (joinFunction.execute(joinedKeyFields, executionCtx.pipelineMemoryProvider.arena))
            {
                auto leftRecord = leftPagedVector.readRecord(leftItemPos, leftFields);
                auto rightRecord = rightPagedVector.readRecord(rightItemPos, rightFields);
                auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd);
                executeChild(executionCtx, joinedRecord);
            }

            ++rightItemPos;
        }
        ++leftItemPos;
    }
}

}
