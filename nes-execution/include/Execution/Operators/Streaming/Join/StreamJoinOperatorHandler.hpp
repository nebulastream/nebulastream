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

#pragma once

#include <map>
#include <memory>
#include <vector>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution::Operators
{
/// This operator is the general join operator handler. It is expected that all StreamJoinOperatorHandlers inherit from this class
class StreamJoinOperatorHandler : public WindowBasedOperatorHandler
{
public:
    StreamJoinOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider);

    int8_t* getStartOfSliceCacheEntries(const WorkerThreadId& workerThreadId, const QueryCompilation::JoinBuildSideType& joinBuildSide);
    void allocateSliceCacheEntries(
      const uint64_t sizeOfEntry,
      const uint64_t numberOfEntries,
      Memory::AbstractBufferProvider *bufferProvider) override;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    /// Emits the left and right slice to the probe
    virtual void emitSliceIdsToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfoAndSequenceNumber& windowInfoAndSeqNumber,
        const ChunkNumber& chunkNumber,
        bool isLastChunk,
        PipelineExecutionContext* pipelineCtx)
        = 0;

    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider;
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider;
};
}
