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
#include <Execution/Operators/SliceAssigner.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution::Operators
{

/// This operator is the general join operator handler. It is expected that all StreamJoinOperatorHandlers inherit from this class
class StreamJoinOperatorHandler : public OperatorHandler
{
public:
    StreamJoinOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        const OriginId outputOriginId,
        const uint64_t windowSize,
        const uint64_t windowSlide,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
        const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider);

    void setWorkerThreads(uint64_t numberOfWorkerThreads);
    void setBufferProvider(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider);

    void start(PipelineExecutionContextPtr, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /// Retrieves the slice that corresponds to the timestamp. If no window exists for the timestamp, one is created
    std::shared_ptr<StreamJoinSlice> getSliceByTimestampOrCreateIt(Timestamp timestamp);

    /// Retrieves the slice by its end timestamp. If no slice exists for the given slice end, the optional return value is nullopt
    std::optional<std::shared_ptr<StreamJoinSlice>> getSliceBySliceEnd(SliceEnd sliceEnd);

    /// Triggers windows that are ready. This method updates the watermarkProcessor and should be thread-safe
    void checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /// Updates the corresponding watermark processor and then garbage collect all slices and windows that are not valid anymore
    void garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData);

    /// Triggers all windows that have not been already emitted to the probe
    void triggerAllWindows(PipelineExecutionContext* pipelineCtx);

    /// Deletes all slices and windows
    void deleteAllSlicesAndWindows();

    [[nodiscard]] uint64_t getNumberOfSlices();
    [[nodiscard]] uint64_t getNextSequenceNumber();
    [[nodiscard]] uint64_t getWindowSize() const;
    [[nodiscard]] uint64_t getWindowSlide() const;
    [[nodiscard]] OriginId getOutputOriginId() const;

private:
    /// Retrieves all window identifiers that correspond to this slice
    std::vector<WindowInfo> getAllWindowInfosForSlice(const StreamJoinSlice& slice) const;

    /// Creates a new slice and possibly window for the given start and end
    virtual std::shared_ptr<StreamJoinSlice> createNewSlice(SliceStart sliceStart, SliceEnd sliceEnd) = 0;

    /// Emits the left and right slice to the probe
    virtual void emitSliceIdsToProbe(
        StreamJoinSlice& sliceLeft, StreamJoinSlice& sliceRight, const WindowInfo& windowInfo, PipelineExecutionContext* pipelineCtx)
        = 0;

protected:
    /// We need to store the windows and slices in two separate maps. This is necessary as we need to access the slices during the join build phase,
    /// while we need to access windows during the triggering of windows.
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windows;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<StreamJoinSlice>>> slices;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorBuild;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorProbe;
    const OriginId outputOriginId;
    uint64_t numberOfWorkerThreads;
    const SliceAssigner sliceAssigner;
    std::atomic<SequenceNumber::Underlying> sequenceNumber;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider;
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider;
};
}
