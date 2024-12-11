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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{

NLJSlice* getNLJSliceRefFromEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    if (const auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd); slice.has_value())
    {
        return dynamic_cast<NLJSlice*>(slice.value().get());
    }

    INVARIANT(false, "Could not find a slice for slice end {}", sliceEnd);
}

Timestamp getNLJWindowStartProxy(const EmittedNLJWindowTriggerTask* nljWindowTriggerTask)
{
    NES_ASSERT2_FMT(nljWindowTriggerTask != nullptr, "nljWindowTriggerTask should not be null");
    return nljWindowTriggerTask->windowInfo.windowStart;
}

Timestamp getNLJWindowEndProxy(const EmittedNLJWindowTriggerTask* nljWindowTriggerTask)
{
    NES_ASSERT2_FMT(nljWindowTriggerTask != nullptr, "nljWindowTriggerTask should not be null");
    return nljWindowTriggerTask->windowInfo.windowEnd;
}

SliceEnd
getNLJSliceEndProxy(const EmittedNLJWindowTriggerTask* nljWindowTriggerTask, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    PRECONDITION(nljWindowTriggerTask != nullptr, "nljWindowTriggerTask should not be null");

    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left:
            return nljWindowTriggerTask->leftSliceEnd;
        case QueryCompilation::JoinBuildSideType::Right:
            return nljWindowTriggerTask->rightSliceEnd;
    }
}

NLJProbe::NLJProbe(
    const uint64_t operatorHandlerIndex,
    const std::shared_ptr<Functions::Function>& joinFunction,
    const WindowMetaData& windowMetaData,
    const JoinSchema& joinSchema,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider)
    : StreamJoinProbe(operatorHandlerIndex, joinFunction, windowMetaData, joinSchema)
    , leftMemoryProvider(leftMemoryProvider)
    , rightMemoryProvider(rightMemoryProvider)
{
}

void NLJProbe::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    Operator::open(executionCtx, recordBuffer);

//     /// Getting all needed info from the recordBuffer
//     const auto nljWindowTriggerTaskRef = static_cast<nautilus::val<EmittedNLJWindowTriggerTask*>>(recordBuffer.getBuffer());
//     const auto sliceIdLeft = invoke(
//         getNLJSliceEndProxy,
//         nljWindowTriggerTaskRef,
//         nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Left));
//     const auto sliceIdRight = invoke(
//         getNLJSliceEndProxy,
//         nljWindowTriggerTaskRef,
//         nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Right));
//     const auto windowStart = invoke(getNLJWindowStartProxy, nljWindowTriggerTaskRef);
//     const auto windowEnd = invoke(getNLJWindowEndProxy, nljWindowTriggerTaskRef);

//     /// During triggering the slice, we append all pages of all local copies to a single PagedVector located at position 0
//     const auto workerThreadIdForPages = nautilus::val<WorkerThreadId>(WorkerThreadId(0));

//     /// Getting the left and right paged vector
//     const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
//     const auto sliceRefLeft = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdLeft);
//     const auto sliceRefRight = invoke(getNLJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdRight);

//     const auto leftPagedVectorRef = invoke(
//         getNLJPagedVectorProxy,
//         sliceRefLeft,
//         workerThreadIdForPages,
//         nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Left));
//     const auto rightPagedVectorRef = invoke(
//         getNLJPagedVectorProxy,
//         sliceRefRight,
//         workerThreadIdForPages,
//         nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Right));

//     const Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftMemoryProvider);
//     const Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightMemoryProvider);

//     const auto leftKeyFields = leftMemoryProvider->getMemoryLayoutPtr()->getKeyFieldNames();
//     const auto rightKeyFields = rightMemoryProvider->getMemoryLayoutPtr()->getKeyFieldNames();
//     const auto leftFields = leftMemoryProvider->getMemoryLayoutPtr()->getSchema()->getFieldNames();
//     const auto rightFields = rightMemoryProvider->getMemoryLayoutPtr()->getSchema()->getFieldNames();

//     nautilus::val<uint64_t> leftItemPos = 0UL;
//     for (auto leftIt = leftPagedVector.begin(leftKeyFields); leftIt != leftPagedVector.end(leftKeyFields); ++leftIt)
//     {
//         nautilus::val<uint64_t> rightItemPos = 0UL;
//         for (auto rightIt = rightPagedVector.begin(rightKeyFields); rightIt != rightPagedVector.end(rightKeyFields); ++rightIt)
//         {
//             auto joinedKeyFields = createJoinedRecord(*leftIt, *rightIt, windowStart, windowEnd, leftKeyFields, rightKeyFields);
//             if (joinFunction->execute(joinedKeyFields))
//             {
//                 auto leftRecord = leftPagedVector.readRecord(leftItemPos, leftFields);
//                 auto rightRecord = rightPagedVector.readRecord(rightItemPos, rightFields);
//                 auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd);
//                 child->execute(executionCtx, joinedRecord);
//             }

//             ++rightItemPos;
//         }
//         ++leftItemPos;
//     }
 }

}
