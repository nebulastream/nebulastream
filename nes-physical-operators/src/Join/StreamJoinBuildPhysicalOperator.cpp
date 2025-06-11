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

#include <memory>
#include <utility>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <Watermark/TimeFunction.hpp>
#include <nautilus/val_enum.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

void updateSlicesProxy(
    OperatorHandler* ptrOpHandler,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const JoinBuildSideType joinBuildSide,
    const WorkerThreadId workerThreadId,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");
    PRECONDITION(bufferProvider != nullptr, "buffer provider should not be null!");
    PRECONDITION(memoryLayout != nullptr, "memory layout should not be null!");

    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);

    if (const auto sliceStore = dynamic_cast<FileBackedTimeBasedSliceStore*>(&opHandler->getSliceAndWindowStore()))
    {
        runSingleAwaitable(
            opHandler->getIoContext(),
            sliceStore->updateSlices(
                opHandler->getIoContext(),
                bufferProvider,
                memoryLayout,
                UpdateSlicesMetaData(
                    workerThreadId,
                    joinBuildSide,
                    BufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId))));
    }
}

StreamJoinBuildPhysicalOperator::StreamJoinBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction))
    , joinBuildSide(joinBuildSide)
    , memoryProvider(std::move(std::move(memoryProvider)))
{
}

void StreamJoinBuildPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    // TODO investigate why some systests arbitrarily fail if we call base class close first
    /// Update the slices in main memory and on external storage device
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        updateSlicesProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<Memory::MemoryLayouts::MemoryLayout*>(memoryProvider->getMemoryLayout().get()),
        nautilus::val<JoinBuildSideType>(joinBuildSide),
        executionCtx.workerThreadId,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);

    /// Call the base class close method to ensure checkWindowsTrigger is called
    WindowBuildPhysicalOperator::close(executionCtx, recordBuffer);
}

}
