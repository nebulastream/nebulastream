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
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Streaming/Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Streaming/WindowBuildPhysicalOperator.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Watermark/TimeFunction.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>

namespace NES
{
SliceStart getNLJSliceStartProxy(const NLJSlice* nljSlice)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    return nljSlice->getSliceStart();
}

SliceEnd getNLJSliceEndProxy(const NLJSlice* nljSlice)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    return nljSlice->getSliceEnd();
}

NLJSlice* getNLJSliceRefProxy(OperatorHandler* ptrOpHandler, const Timestamp timestamp)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    const auto createFunction = opHandler->getCreateNewSlicesFunction();
    return dynamic_cast<NLJSlice*>(opHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, createFunction)[0].get());
}

NLJBuildPhysicalOperator::NLJBuildPhysicalOperator(
    std::shared_ptr<TupleBufferMemoryProvider> memoryProvider,
    const uint64_t operatorHandlerIndex,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction)
    : StreamJoinBuildPhysicalOperator(std::move(memoryProvider), operatorHandlerIndex, joinBuildSide, std::move(timeFunction))
{
}

void NLJBuildPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    WindowBuildPhysicalOperator::open(executionCtx, recordBuffer);

    auto opHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceReference = invoke(getNLJSliceRefProxy, opHandlerMemRef, recordBuffer.getWatermarkTs());
    auto sliceStart = invoke(getNLJSliceStartProxy, sliceReference);
    auto sliceEnd = invoke(getNLJSliceEndProxy, sliceReference);
    const auto pagedVectorReference = invoke(
        getNLJPagedVectorProxy,
        sliceReference,
        executionCtx.getWorkerThreadId(),
        nautilus::val<JoinBuildSideType>(joinBuildSide));


    auto localJoinState
        = std::make_unique<LocalNestedLoopJoinState>(opHandlerMemRef, sliceReference, sliceStart, sliceEnd, pagedVectorReference);

    /// Store the local state
    executionCtx.setLocalOperatorState(id, std::move(localJoinState));
}

void NLJBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Get the current join state that stores the slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto* localJoinState = getLocalJoinState(executionCtx, timestamp);

    /// Write record to the pagedVector
    const Interface::PagedVectorRef pagedVectorRef(
        localJoinState->nljPagedVectorMemRef, memoryProvider, executionCtx.pipelineMemoryProvider.bufferProvider);
    pagedVectorRef.writeRecord(record);
}

NLJBuildPhysicalOperator::LocalNestedLoopJoinState*
NLJBuildPhysicalOperator::getLocalJoinState(ExecutionContext& executionCtx, const nautilus::val<Timestamp>& timestamp) const
{
    auto* localJoinState = dynamic_cast<LocalNestedLoopJoinState*>(executionCtx.getLocalState(id));
    const auto operatorHandlerMemRef = localJoinState->joinOperatorHandler;

    if (!(localJoinState->sliceStart <= timestamp && timestamp < localJoinState->sliceEnd))
    {
        /// Get the slice for the current timestamp
        updateLocalJoinState(localJoinState, operatorHandlerMemRef, executionCtx, timestamp);
    }
    return localJoinState;
}

void NLJBuildPhysicalOperator::updateLocalJoinState(
    LocalNestedLoopJoinState* localJoinState,
    const nautilus::val<NLJOperatorHandler*>& operatorHandlerRef,
    const ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& timestamp) const
{
    localJoinState->sliceReference = invoke(getNLJSliceRefProxy, operatorHandlerRef, timestamp);
    localJoinState->sliceStart = invoke(getNLJSliceStartProxy, localJoinState->sliceReference);
    localJoinState->sliceEnd = invoke(getNLJSliceEndProxy, localJoinState->sliceReference);
    localJoinState->nljPagedVectorMemRef = invoke(
        getNLJPagedVectorProxy,
        localJoinState->sliceReference,
        executionCtx.getWorkerThreadId(),
        nautilus::val<JoinBuildSideType>(joinBuildSide));
}
}
