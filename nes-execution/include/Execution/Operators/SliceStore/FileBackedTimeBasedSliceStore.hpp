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

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <vector>
#include <Execution/Operators/SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/MemoryController/MemoryController.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Execution/Operators/SliceStore/WatermarkPredictor/AbstractWatermarkPredictor.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution
{

enum DiskOperation : uint8_t
{
    READ,
    WRITE
};

struct MemoryControllerInfo
{
    std::filesystem::path workingDir;
    QueryId queryId;
    OriginId outputOriginId;
};

struct SliceStoreMetaData
{
    WorkerThreadId threadId;
    BufferMetaData bufferMetaData;
};

class FileBackedTimeBasedSliceStore final : public WindowSlicesStoreInterface
{
public:
    //static constexpr std::array<size_t, 10> USE_TEST_DATA_SIZES = {4096, 16384, 65536, 131072, 524288, 1048576, 134217728, 536870912, 1073741824, 2147483648};
    static constexpr std::array<size_t, 6> USE_TEST_DATA_SIZES = {4096, 16384, 65536, 131072, 524288, 1048576};
    static constexpr auto USE_FILE_LAYOUT = NO_SEPARATION;
    static constexpr auto USE_MIN_STATE_SIZE_WRITE
        = 0UL; /// slices with state sice less than 0B for a given ThreadId are not written to external storage
    static constexpr auto USE_MIN_STATE_SIZE_READ
        = 0UL; /// slices with state sice less than 0B for a given ThreadId are not read back from external storage
    static constexpr auto USE_TIME_DELTA_MS = 0UL; /// time delta in ms added to estimated read and write timestamps
    static constexpr auto USE_BUFFER_SIZE = 1024 * 4UL; /// 4 KB size of file write and read buffers
    static constexpr auto USE_NUM_WRITE_BUFFERS = 128UL; /// num file write buffers
    static constexpr auto USE_MAX_NUM_SEQ_NUMBERS = UINT64_MAX; /// max number of data points for predictions
    static constexpr auto USE_NUM_GAPS_ALLOWED = 10UL; /// number of gaps allowed in data points of sequence numbers

    FileBackedTimeBasedSliceStore(
        uint64_t windowSize,
        uint64_t windowSlide,
        MemoryControllerInfo memoryControllerInfo,
        WatermarkPredictorType watermarkPredictorType,
        const std::vector<OriginId>& inputOrigins);
    FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore& other);
    FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore&& other) noexcept;
    FileBackedTimeBasedSliceStore& operator=(FileBackedTimeBasedSliceStore& other);
    FileBackedTimeBasedSliceStore& operator=(FileBackedTimeBasedSliceStore&& other) noexcept;
    ~FileBackedTimeBasedSliceStore() override;

    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp,
        WorkerThreadId workerThreadId,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(
        SliceEnd sliceEnd,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        QueryCompilation::JoinBuildSideType joinBuildSide) override;
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    void deleteState() override;
    uint64_t getWindowSize() const override;
    void setWorkerThreads(uint64_t numberOfWorkerThreads) override;

    void updateSlices(
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        const SliceStoreMetaData& metaData);

private:
    std::vector<std::tuple<std::shared_ptr<Slice>, DiskOperation, FileLayout>> getSlicesToUpdate(
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        WorkerThreadId threadId);

    void readSliceFromFiles(
        const std::shared_ptr<Slice>& slice,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        QueryCompilation::JoinBuildSideType joinBuildSide);

    void updateWatermarkPredictor(OriginId originId);
    void measureReadAndWriteExecTimes(const std::array<size_t, USE_TEST_DATA_SIZES.size()>& dataSizes);

    static uint64_t getExecTimesForDataSize(const std::pair<double, double>& execTimeFunction, const size_t dataSize)
    {
        return static_cast<uint64_t>(execTimeFunction.first * dataSize + execTimeFunction.second);
    }

    static bool isPolynomial(const uint64_t counter)
    {
        /// Checks if counter matches y = n^2
        const auto root = static_cast<uint64_t>(std::sqrt(counter));
        return root * root == counter;
    }
    static bool isExponential(const uint64_t counter)
    {
        /// Checks if counter matches y = 2^n - 1
        return (counter & counter + 1) == 0;
    }

    /// Retrieves all window identifiers that correspond to this slice
    std::vector<WindowInfo> getAllWindowInfosForSlice(const Slice& slice) const;

    /// The watermark processor and predictors are used to predict when a slice should be written out or read back during the build phase.
    std::shared_ptr<Operators::MultiOriginWatermarkProcessor> watermarkProcessor;
    std::map<OriginId, std::shared_ptr<AbstractWatermarkPredictor>> watermarkPredictors;

    /// The pairs hold the slope and intercept needed to calculate execution times of write or read operations for certain data sizes
    /// which is also used for predictions.
    std::pair<double, double> writeExecTimeFunction;
    std::pair<double, double> readExecTimeFunction;

    /// The Memory Controller manages the creation and destruction of FileReader and FileWriter instances and controls the internal memory
    /// pool used by them. It also stores the FileLayout used for each file. The map keeps track of whether slices are in main memory.
    std::shared_ptr<MemoryController> memoryController;
    folly::Synchronized<std::map<std::pair<SliceEnd, QueryCompilation::JoinBuildSideType>, bool>> slicesInMemory;
    std::map<WorkerThreadId, std::map<QueryCompilation::JoinBuildSideType, std::vector<std::shared_ptr<Slice>>>> alteredSlicesPerThread;

    uint64_t numberOfWorkerThreads;
    MemoryControllerInfo memoryControllerInfo;
    std::map<OriginId, std::atomic<uint64_t>> watermarkPredictorUpdateCnt;

    /// We need to store the windows and slices in two separate maps. This is necessary as we need to access the slices during the join build phase,
    /// while we need to access windows during the triggering of windows.
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windows;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<Slice>>> slices;
    Operators::SliceAssigner sliceAssigner;

    /// We need to store the sequence number for the triggerable window infos. This is necessary, as we have to ensure that the sequence number is unique
    /// and increases for each window info.
    std::atomic<SequenceNumber::Underlying> sequenceNumber;

    /// Depending on the number of origins, we have to handle the slices differently.
    /// For example, in getAllNonTriggeredSlices(), we have to wait until all origins have called this method to ensure correctness
    /// The numberOfActiveOrigins shall be guarded by the windows Mutex.
    uint64_t numberOfActiveOrigins;
};

}
