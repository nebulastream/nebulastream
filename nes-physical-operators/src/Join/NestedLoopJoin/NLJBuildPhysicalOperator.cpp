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
#include <Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>

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

NLJBuildPhysicalOperator::NLJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : StreamJoinBuildPhysicalOperator{
          operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(tupleLayout), std::move(sliceStoreRef)}
{
}

void NLJBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    auto nljPagedVectorBuffer = sliceStoreRef->getDataStructureRef(
        timestamp, executionCtx.workerThreadId, operatorHandler, executionCtx.pipelineMemoryProvider.bufferProvider);
    /// Write record to the pagedVector
    PagedVectorRef pagedVectorRef{BorrowedNautilusBuffer::from(nljPagedVectorBuffer.asArg()), tupleLayout};
    pagedVectorRef.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}
}
