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

#include <Join/NestedLoopJoin/NLJProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
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
NLJSlice* getNLJSliceRefFromEndProxy(
    OperatorHandler* ptrOpHandler,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const JoinBuildSideType joinBuildSide,
    const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    PRECONDITION(ptrOpHandler != nullptr, "op handler should not be null");
    PRECONDITION(bufferProvider != nullptr, "buffer provider should not be null!");
    PRECONDITION(memoryLayout != nullptr, "memory layout should not be null!");

    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);

    const auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd, bufferProvider, memoryLayout, joinBuildSide);
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

SliceEnd getNLJSliceEndProxy(const EmittedNLJWindowTrigger* nljWindowTriggerTask, const JoinBuildSideType joinBuildSide)
{
    PRECONDITION(nljWindowTriggerTask != nullptr, "nljWindowTriggerTask should not be null");

    switch (joinBuildSide)
    {
        case JoinBuildSideType::Left:
            return nljWindowTriggerTask->leftSliceEnd;
        case JoinBuildSideType::Right:
            return nljWindowTriggerTask->rightSliceEnd;
    }
    std::unreachable();
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

void NLJProbePhysicalOperator::performNLJ(
    const Interface::PagedVectorRef& outerPagedVector,
    const Interface::PagedVectorRef& innerPagedVector,
    Interface::MemoryProvider::TupleBufferMemoryProvider& outerMemoryProvider,
    Interface::MemoryProvider::TupleBufferMemoryProvider& innerMemoryProvider,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto outerKeyFields = outerMemoryProvider.getMemoryLayout()->getKeyFieldNames();
    const auto innerKeyFields = innerMemoryProvider.getMemoryLayout()->getKeyFieldNames();
    const auto outerFields = outerMemoryProvider.getMemoryLayout()->getSchema().getFieldNames();
    const auto innerFields = innerMemoryProvider.getMemoryLayout()->getSchema().getFieldNames();

    nautilus::val<uint64_t> outerItemPos(0);
    for (auto outerIt = outerPagedVector.begin(outerKeyFields); outerIt != outerPagedVector.end(outerKeyFields); ++outerIt)
    {
        nautilus::val<uint64_t> innerItemPos(0);
        for (auto innerIt = innerPagedVector.begin(innerKeyFields); innerIt != innerPagedVector.end(innerKeyFields); ++innerIt)
        {
            const auto joinedKeyFields = createJoinedRecord(*outerIt, *innerIt, windowStart, windowEnd, outerKeyFields, innerKeyFields);
            if (joinFunction.execute(joinedKeyFields, executionCtx.pipelineMemoryProvider.arena))
            {
                auto outerRecord = outerPagedVector.readRecord(outerItemPos, outerFields);
                auto innerRecord = innerPagedVector.readRecord(innerItemPos, innerFields);
                auto joinedRecord = createJoinedRecord(outerRecord, innerRecord, windowStart, windowEnd, outerFields, innerFields);
                executeChild(executionCtx, joinedRecord);
            }

            ++innerItemPos;
        }
        ++outerItemPos;
    }
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
    const auto sliceRefLeft = invoke(
        getNLJSliceRefFromEndProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<Memory::MemoryLayouts::MemoryLayout*>(leftMemoryProvider->getMemoryLayout().get()),
        nautilus::val<JoinBuildSideType>(JoinBuildSideType::Left),
        sliceIdLeft);
    const auto sliceRefRight = invoke(
        getNLJSliceRefFromEndProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<Memory::MemoryLayouts::MemoryLayout*>(rightMemoryProvider->getMemoryLayout().get()),
        nautilus::val<JoinBuildSideType>(JoinBuildSideType::Right),
        sliceIdRight);

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

    const Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftMemoryProvider);
    const Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightMemoryProvider);
    const auto numberOfTuplesLeft = leftPagedVector.getNumberOfTuples();
    const auto numberOfTuplesRight = rightPagedVector.getNumberOfTuples();

    /// Outer loop should have more no. tuples
    if (numberOfTuplesLeft < numberOfTuplesRight)
    {
        performNLJ(leftPagedVector, rightPagedVector, *leftMemoryProvider, *rightMemoryProvider, executionCtx, windowStart, windowEnd);
    }
    else
    {
        performNLJ(rightPagedVector, leftPagedVector, *rightMemoryProvider, *leftMemoryProvider, executionCtx, windowStart, windowEnd);
    }
}

}
