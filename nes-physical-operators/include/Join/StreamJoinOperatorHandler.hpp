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
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
/// This operator is the general join operator handler. It is expected that all StreamJoinOperatorHandlers inherit from this class
class StreamJoinOperatorHandler : public WindowBasedOperatorHandler
{
public:
    StreamJoinOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);

    void allocateSliceCacheEntries(
        const uint64_t sizeOfEntry, const uint64_t numberOfEntries, Memory::AbstractBufferProvider* bufferProvider) override;

    struct StartSliceCacheEntriesStreamJoin final : StartSliceCacheEntriesArgs
    {
        JoinBuildSideType joinBuildSide;

        explicit StartSliceCacheEntriesStreamJoin(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
            : StartSliceCacheEntriesArgs(workerThreadId), joinBuildSide(joinBuildSide)
        {
        }
        StartSliceCacheEntriesStreamJoin(StartSliceCacheEntriesStreamJoin&& other) = default;
        StartSliceCacheEntriesStreamJoin& operator=(StartSliceCacheEntriesStreamJoin&& other) = default;
        StartSliceCacheEntriesStreamJoin(const StartSliceCacheEntriesStreamJoin& other)
            : StartSliceCacheEntriesArgs(other.workerThreadId), joinBuildSide(other.joinBuildSide)
        {
        }
        StartSliceCacheEntriesStreamJoin& operator=(const StartSliceCacheEntriesStreamJoin& other)
        {
            joinBuildSide = other.joinBuildSide;
            workerThreadId = other.workerThreadId;
            return *this;
        };
        ~StartSliceCacheEntriesStreamJoin() override = default;
    };
    const int8_t* getStartOfSliceCacheEntries(const StartSliceCacheEntriesArgs& startSliceCacheEntriesArgs) const override;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    /// Emits the left and right slice to the probe
    virtual void emitSlicesToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx)
        = 0;
};
}
