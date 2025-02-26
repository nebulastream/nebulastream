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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
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

NLJSlice*
getNLJSliceRefProxy(OperatorHandler* ptrOpHandler, const Timestamp timestamp, const Memory::AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(bufferProvider != nullptr, "buffer provider should not be null!");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    const auto createFunction = opHandler->getCreateNewSlicesFunction(bufferProvider);
    return dynamic_cast<NLJSlice*>(opHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, createFunction)[0].get());
}

NLJBuild::NLJBuild(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : StreamJoinBuild(operatorHandlerIndex, joinBuildSide, std::move(timeFunction), memoryProvider)
{
}

void NLJBuild::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Get the current join state that stores the slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto operatorHandlerRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto sliceReference
        = invoke(getNLJSliceRefProxy, operatorHandlerRef, timestamp, executionCtx.pipelineMemoryProvider.bufferProvider);
    const auto nljPagedVectorMemRef = invoke(
        getNLJPagedVectorProxy,
        sliceReference,
        executionCtx.getWorkerThreadId(),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));


    /// Write record to the pagedVector
    const Interface::PagedVectorRef pagedVectorRef(
        nljPagedVectorMemRef, memoryProvider, executionCtx.pipelineMemoryProvider.bufferProvider);
    pagedVectorRef.writeRecord(record);
}
}
