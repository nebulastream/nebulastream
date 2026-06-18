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

#include <Join/IntervalJoin/IntervalJoinBuildPhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

IntervalJoinBuildPhysicalOperator::IntervalJoinBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<TupleBufferRef> bufferRef,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : StreamJoinBuildPhysicalOperator{
          operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(bufferRef), std::move(sliceStoreRef)}
{
}

void IntervalJoinBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto pagedVectorMemRef = sliceStoreRef->getDataStructureRef(timestamp, executionCtx.workerThreadId, operatorHandler);

    const PagedVectorRef pagedVectorRef{pagedVectorMemRef, bufferRef};
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}

}
