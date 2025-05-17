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

NLJSlice* getNLJSliceRefFromEndProxy(
    OperatorHandler* ptrOpHandler,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler should not be null");
    PRECONDITION(bufferProvider != nullptr, "buffer provider should not be null!");
    PRECONDITION(memoryLayout != nullptr, "memory layout should not be null!");

    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);

    const auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd, bufferProvider, memoryLayout, joinBuildSide);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);

    /// For creating memory usage metrics
    const auto now = std::chrono::high_resolution_clock::now();
    std::cout << std::format(
        "{},{},{}\n",
        bufferProvider->getAvailableBuffers(),
        bufferProvider->getNumOfUnpooledBuffers(),
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count()));
    /*if (const auto numPooledBuffers = bufferProvider->getNumOfPooledBuffers(); numPooledBuffers != 200000)
    {
        std::cout << std::format("NumPooledBuffers={}\n", numPooledBuffers);
    }*/

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

SliceEnd getNLJSliceEndProxy(const EmittedNLJWindowTrigger* nljWindowTriggerTask, const QueryCompilation::JoinBuildSideType joinBuildSide)
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

    /// Getting all needed info from the recordBuffer
    const auto nljWindowTriggerTaskRef = static_cast<nautilus::val<EmittedNLJWindowTrigger*>>(recordBuffer.getBuffer());
    const auto sliceIdLeft = invoke(
        getNLJSliceEndProxy,
        nljWindowTriggerTaskRef,
        nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Left));
    const auto sliceIdRight = invoke(
        getNLJSliceEndProxy,
        nljWindowTriggerTaskRef,
        nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Right));
    const auto windowStart = invoke(getNLJWindowStartProxy, nljWindowTriggerTaskRef);
    const auto windowEnd = invoke(getNLJWindowEndProxy, nljWindowTriggerTaskRef);

    /// During triggering the slice, we append all pages of all local copies to a single PagedVector located at position 0
    const auto workerThreadIdForPages = nautilus::val<WorkerThreadId>(WorkerThreadId(0));

    /// Getting the left and right paged vector
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto sliceRefLeft = invoke(
        getNLJSliceRefFromEndProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<Memory::MemoryLayouts::MemoryLayout*>(leftMemoryProvider->getMemoryLayout().get()),
        nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Left),
        sliceIdLeft);
    const auto sliceRefRight = invoke(
        getNLJSliceRefFromEndProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<Memory::MemoryLayouts::MemoryLayout*>(rightMemoryProvider->getMemoryLayout().get()),
        nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Right),
        sliceIdRight);

    const auto leftPagedVectorRef = invoke(
        getNLJPagedVectorProxy,
        sliceRefLeft,
        workerThreadIdForPages,
        nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Left));
    const auto rightPagedVectorRef = invoke(
        getNLJPagedVectorProxy,
        sliceRefRight,
        workerThreadIdForPages,
        nautilus::val<QueryCompilation::JoinBuildSideType>(QueryCompilation::JoinBuildSideType::Right));

    const Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftMemoryProvider);
    const Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightMemoryProvider);

    const auto leftKeyFields = leftMemoryProvider->getMemoryLayout()->getKeyFieldNames();
    const auto rightKeyFields = rightMemoryProvider->getMemoryLayout()->getKeyFieldNames();
    const auto leftFields = leftMemoryProvider->getMemoryLayout()->getSchema()->getFieldNames();
    const auto rightFields = rightMemoryProvider->getMemoryLayout()->getSchema()->getFieldNames();

    nautilus::val<uint64_t> leftItemPos = 0UL;
    for (auto leftIt = leftPagedVector.begin(leftKeyFields); leftIt != leftPagedVector.end(leftKeyFields); ++leftIt)
    {
        nautilus::val<uint64_t> rightItemPos = 0UL;
        for (auto rightIt = rightPagedVector.begin(rightKeyFields); rightIt != rightPagedVector.end(rightKeyFields); ++rightIt)
        {
            auto joinedKeyFields = createJoinedRecord(*leftIt, *rightIt, windowStart, windowEnd, leftKeyFields, rightKeyFields);
            if (joinFunction->execute(joinedKeyFields, executionCtx.pipelineMemoryProvider.arena))
            {
                auto leftRecord = leftPagedVector.readRecord(leftItemPos, leftFields);
                auto rightRecord = rightPagedVector.readRecord(rightItemPos, rightFields);
                auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd);
                child->execute(executionCtx, joinedRecord);
            }

            ++rightItemPos;
        }
        ++leftItemPos;
    }
}

}
