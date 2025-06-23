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
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/MemoryController/MemoryController.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WatermarkPredictor/AbstractWatermarkPredictor.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

enum class FileOperation : uint8_t
{
    READ,
    WRITE
};

struct SliceStoreInfo
{
    uint64_t lowerMemoryBound;
    uint64_t upperMemoryBound;
    uint64_t fileDescriptorBufferSize;
    uint64_t maxNumWatermarkGaps;
    uint64_t maxNumSequenceNumbers;
    uint64_t minReadStateSize;
    uint64_t minWriteStateSize;
    uint64_t fileOperationTimeDelta;
    FileLayout fileLayout;
    bool withPrediction;
};

struct MemoryControllerInfo
{
    std::filesystem::path workingDir;
    QueryId queryId;
    OriginId outputOriginId;
};

struct UpdateSlicesMetaData
{
    WorkerThreadId threadId;
    JoinBuildSideType joinBuildSide;
    BufferMetaData bufferMetaData;
};

class FileBackedTimeBasedSliceStore final : public DefaultTimeBasedSliceStore
{
public:
    //static constexpr std::array<size_t, 10> USE_TEST_DATA_SIZES = {4096, 16384, 65536, 131072, 524288, 1048576, 134217728, 536870912, 1073741824, 2147483648};
    static constexpr std::array<size_t, 6> USE_TEST_DATA_SIZES = {4096, 16384, 65536, 131072, 524288, 1048576};

    FileBackedTimeBasedSliceStore(
        uint64_t windowSize,
        uint64_t windowSlide,
        const SliceStoreInfo& sliceStoreInfo,
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
        JoinBuildSideType joinBuildSide,
        const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(
        SliceEnd sliceEnd,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        JoinBuildSideType joinBuildSide) override;
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    void deleteState() override;

    /// Sets number of worker threads. Needs to be called before working with the slice store
    void setWorkerThreads(uint64_t numberOfWorkerThreads);

    /// TODO
    boost::asio::awaitable<void> updateSlices(
        boost::asio::io_context& ioCtx,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        const UpdateSlicesMetaData& metaData);

private:
    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> getSlicesToUpdate(
        const Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        Timestamp watermark,
        WorkerThreadId threadId,
        JoinBuildSideType joinBuildSide);

    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>>
    updateSlicesReactive(WorkerThreadId threadId, JoinBuildSideType joinBuildSide);
    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>>
    updateSlicesProactiveWithoutPrediction(Timestamp watermark, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);
    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> updateSlicesProactiveWithPrediction(
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

    void readSliceFromFiles(
        const std::shared_ptr<Slice>& slice,
        Memory::AbstractBufferProvider* bufferProvider,
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
        JoinBuildSideType joinBuildSide);

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

    /// The watermark processor and predictors are used to predict when a slice should be written out or read back during the build phase.
    std::shared_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::map<OriginId, std::shared_ptr<AbstractWatermarkPredictor>> watermarkPredictors;
    std::mutex readWriteMutex;

    /// The pairs hold the slope and intercept needed to calculate execution times of write or read operations for certain data sizes
    /// which is also used for predictions.
    std::pair<double, double> writeExecTimeFunction;
    std::pair<double, double> readExecTimeFunction;

    /// The Memory Controller manages the creation and destruction of FileReader and FileWriter instances and controls the internal memory
    /// pool used by them. It also stores the FileLayout used for each file. The map keeps track of whether slices are in main memory. TODO
    std::shared_ptr<MemoryController> memoryController;
    std::vector<std::map<std::pair<SliceEnd, JoinBuildSideType>, bool>> slicesInMemory;
    std::vector<std::mutex> slicesInMemoryMutexes;
    std::map<std::pair<WorkerThreadId, JoinBuildSideType>, std::vector<std::shared_ptr<Slice>>> alteredSlicesPerThread;

    SliceStoreInfo sliceStoreInfo;
    uint64_t numberOfWorkerThreads;
    MemoryControllerInfo memoryControllerInfo;
    std::map<OriginId, std::atomic<uint64_t>> watermarkPredictorUpdateCnt;
};

}
