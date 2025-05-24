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
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
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

NLJBuildPhysicalOperator::NLJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : StreamJoinBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(memoryProvider))
{
}

void NLJBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto sliceReference = invoke(
        +[](OperatorHandler* ptrOpHandler,
            const Timestamp timestampVal,
            const WorkerThreadId workerThreadId,
            const JoinBuildSideType joinBuildSide)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
            const auto createFunction = opHandler->getCreateNewSlicesFunction();
            return dynamic_cast<NLJSlice*>(opHandler->getSliceAndWindowStore()
                                               .getSlicesOrCreate(timestampVal, workerThreadId, joinBuildSide, createFunction)[0]
                                               .get());
        },
        operatorHandler,
        timestamp,
        executionCtx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide));
    const auto nljPagedVectorMemRef = invoke(
        +[](const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
        {
            PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
            return nljSlice->getPagedVectorRef(workerThreadId, joinBuildSide);
        },
        sliceReference,
        executionCtx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide));


    /// Write record to the pagedVector
    const Interface::PagedVectorRef pagedVectorRef(nljPagedVectorMemRef, memoryProvider);
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}
}
