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
#include <Execution/Operators/SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Util/Execution.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <nautilus/val_enum.hpp>

namespace NES::Runtime::Execution::Operators
{

void updateSlicesProxy(
    OperatorHandler* ptrOpHandler,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
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
                joinBuildSide,
                UpdateSlicesMetaData(
                    workerThreadId, BufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId))));
    }
}

StreamJoinBuild::StreamJoinBuild(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : WindowOperatorBuild(operatorHandlerIndex, std::move(timeFunction)), joinBuildSide(joinBuildSide), memoryProvider(memoryProvider)
{
}

void StreamJoinBuild::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    // TODO investigate why some systests arbitrarily fail if we call base class close first
    /// Update the slices in main memory and on external storage device
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        updateSlicesProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<Memory::MemoryLayouts::MemoryLayout*>(memoryProvider->getMemoryLayout().get()),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide),
        executionCtx.getWorkerThreadId(),
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);

    /// Call the base class close method to ensure checkWindowsTrigger is called
    WindowOperatorBuild::close(executionCtx, recordBuffer);
}

}
