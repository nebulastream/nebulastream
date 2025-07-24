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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WatermarkPredictor/KalmanBasedWindowTriggerPredictor.hpp>
#include <SliceStore/WatermarkPredictor/RLSBasedWatermarkPredictor.hpp>
#include <SliceStore/WatermarkPredictor/RegressionBasedWatermarkPredictor.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Locks.hpp>
#include <folly/Synchronized.h>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(
    const uint64_t windowSize,
    const uint64_t windowSlide,
    const SliceStoreInfo& sliceStoreInfo,
    MemoryControllerInfo memoryControllerInfo,
    const WatermarkPredictorType watermarkPredictorType,
    const std::vector<OriginId>& inputOrigins)
    : DefaultTimeBasedSliceStore(windowSize, windowSlide, inputOrigins.size())
    , watermarkProcessor(std::make_shared<MultiOriginWatermarkProcessor>(inputOrigins))
    , sliceStoreInfo(sliceStoreInfo)
    , numberOfWorkerThreads(0)
    , memoryControllerInfo(std::move(memoryControllerInfo))
{
    for (const auto origin : inputOrigins)
    {
        watermarkPredictorUpdateCnt.emplace(origin, 0);
        switch (watermarkPredictorType)
        {
            case WatermarkPredictorType::KALMAN: {
                watermarkPredictors.emplace(origin, std::make_shared<KalmanWindowTriggerPredictor>());
                break;
            }
            case WatermarkPredictorType::REGRESSION: {
                watermarkPredictors.emplace(origin, std::make_shared<RegressionBasedWatermarkPredictor>());
                break;
            }
            case WatermarkPredictorType::RLS: {
                watermarkPredictors.emplace(origin, std::make_shared<RLSBasedWatermarkPredictor>());
                break;
            }
        }
    }
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore& other)
    : DefaultTimeBasedSliceStore(other)
    , watermarkProcessor(other.watermarkProcessor)
    , watermarkPredictors(other.watermarkPredictors)
    , writeExecTimeFunction(other.writeExecTimeFunction)
    , readExecTimeFunction(other.readExecTimeFunction)
    , memoryController(other.memoryController)
    , alteredSlicesPerThread(other.alteredSlicesPerThread)
    , sliceStoreInfo(other.sliceStoreInfo)
    , numberOfWorkerThreads(other.numberOfWorkerThreads)
    , memoryControllerInfo(other.memoryControllerInfo)
{
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesInMemoryLocked = *otherSlicesInMemoryLocked;

    for (const auto& [origin, count] : other.watermarkPredictorUpdateCnt)
    {
        watermarkPredictorUpdateCnt.emplace(origin, count.load());
    }
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore&& other) noexcept
    : DefaultTimeBasedSliceStore(std::move(other))
    , watermarkProcessor(std::move(other.watermarkProcessor))
    , watermarkPredictors(std::move(other.watermarkPredictors))
    , writeExecTimeFunction(std::move(other.writeExecTimeFunction))
    , readExecTimeFunction(std::move(other.readExecTimeFunction))
    , memoryController(std::move(other.memoryController))
    , alteredSlicesPerThread(std::move(other.alteredSlicesPerThread))
    , sliceStoreInfo(std::move(other.sliceStoreInfo))
    , numberOfWorkerThreads(std::move(other.numberOfWorkerThreads))
    , memoryControllerInfo(std::move(other.memoryControllerInfo))
    , watermarkPredictorUpdateCnt(std::move(other.watermarkPredictorUpdateCnt))
{
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesInMemoryLocked = std::move(*otherSlicesInMemoryLocked);
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore& other)
{
    DefaultTimeBasedSliceStore::operator=(other);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesInMemoryLocked = *otherSlicesInMemoryLocked;

    for (const auto& [origin, count] : other.watermarkPredictorUpdateCnt)
    {
        watermarkPredictorUpdateCnt.emplace(origin, count.load());
    }

    watermarkProcessor = other.watermarkProcessor;
    watermarkPredictors = other.watermarkPredictors;
    writeExecTimeFunction = other.writeExecTimeFunction;
    readExecTimeFunction = other.readExecTimeFunction;
    memoryController = other.memoryController;
    alteredSlicesPerThread = other.alteredSlicesPerThread;
    sliceStoreInfo = other.sliceStoreInfo;
    numberOfWorkerThreads = other.numberOfWorkerThreads;
    memoryControllerInfo = other.memoryControllerInfo;
    return *this;
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore&& other) noexcept
{
    DefaultTimeBasedSliceStore::operator=(std::move(other));
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesInMemoryLocked = std::move(*otherSlicesInMemoryLocked);

    watermarkProcessor = std::move(other.watermarkProcessor);
    watermarkPredictors = std::move(other.watermarkPredictors);
    writeExecTimeFunction = std::move(other.writeExecTimeFunction);
    readExecTimeFunction = std::move(other.readExecTimeFunction);
    memoryController = std::move(other.memoryController);
    alteredSlicesPerThread = std::move(other.alteredSlicesPerThread);
    sliceStoreInfo = std::move(other.sliceStoreInfo);
    numberOfWorkerThreads = std::move(other.numberOfWorkerThreads);
    memoryControllerInfo = std::move(other.memoryControllerInfo);
    watermarkPredictorUpdateCnt = std::move(other.watermarkPredictorUpdateCnt);
    return *this;
}

FileBackedTimeBasedSliceStore::~FileBackedTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType joinBuildSide,
    const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    const auto threadId = WorkerThreadId(workerThreadId % numberOfWorkerThreads);
    const auto& slicesVec = DefaultTimeBasedSliceStore::getSlicesOrCreate(timestamp, workerThreadId, joinBuildSide, createNewSlice);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    for (const auto& slice : slicesVec)
    {
        alteredSlicesPerThread[{threadId, joinBuildSide}].emplace_back(slice);
        if (const auto sliceEnd = slice->getSliceEnd(); !slicesInMemoryLocked->contains({sliceEnd, joinBuildSide}))
        {
            (*slicesInMemoryLocked)[{sliceEnd, joinBuildSide}] = true;
        }
    }
    return slicesVec;
}

std::optional<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSliceBySliceEnd(
    const SliceEnd sliceEnd,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const JoinBuildSideType joinBuildSide)
{
    const auto& slice = DefaultTimeBasedSliceStore::getSliceBySliceEnd(sliceEnd, bufferProvider, memoryLayout, joinBuildSide);
    if (slice.has_value())
    {
        readSliceFromFiles(slice.value(), bufferProvider, memoryLayout, joinBuildSide);
    }
    return slice;
}

void FileBackedTimeBasedSliceStore::garbageCollectSlicesAndWindows(const Timestamp newGlobalWaterMark)
{
    std::vector<std::shared_ptr<Slice>> slicesToDelete;
    NES_TRACE("Performing garbage collection for new global watermark {}", newGlobalWaterMark);

    /// Solely acquiring a lock for the windows
    if (const auto windowsWriteLocked = windows.tryWLock())
    {
        /// 1. We iterate over all windows and erase them if they can be deleted
        /// This condition is true, if the window end is smaller than the new global watermark of the probe phase.
        for (auto windowsLockedIt = windowsWriteLocked->begin(); windowsLockedIt != windowsWriteLocked->end();)
        {
            const auto& [windowInfo, windowSlicesAndState] = *windowsLockedIt;
            if (windowInfo.windowEnd < newGlobalWaterMark and windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
            {
                windowsLockedIt = windowsWriteLocked->erase(windowsLockedIt);
            }
            else if (windowInfo.windowEnd > newGlobalWaterMark)
            {
                /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
                break;
            }
            else
            {
                ++windowsLockedIt;
            }
        }
    }

    /// Solely acquiring a lock for the slices
    if (const auto slicesWriteLocked = slices.tryWLock())
    {
        /// 2. We gather all slices if they are not used in any window that has not been triggered/can not be deleted yet
        for (auto slicesLockedIt = slicesWriteLocked->begin(); slicesLockedIt != slicesWriteLocked->end();)
        {
            const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
            if (sliceEnd + sliceAssigner.getWindowSize() < newGlobalWaterMark)
            {
                NES_TRACE("Deleting slice with sliceEnd {} as it is not used anymore", sliceEnd);
                /// As we are first copying the shared_ptr the destructor of Slice will not be called.
                /// This allows us to solely collect what slices to delete during holding the lock, while the time-consuming destructor is called without holding any locks
                slicesToDelete.emplace_back(slicePtr);
                slicesLockedIt = slicesWriteLocked->erase(slicesLockedIt);
            }
            else
            {
                /// As the slices are sorted (due to std::map), we can break here as we will not find any slices with a smaller slice end
                break;
            }
        }
    }

    {
        const auto slicesInMemoryLocked = slicesInMemory.wlock();
        for (const auto& slice : slicesToDelete)
        {
            const auto sliceEnd = slice->getSliceEnd();
            memoryController->deleteSliceFiles(sliceEnd);
            slicesInMemoryLocked->erase({sliceEnd, JoinBuildSideType::Left});
            slicesInMemoryLocked->erase({sliceEnd, JoinBuildSideType::Right});
        }
    }

    /// Now we can remove/call destructor on every slice without still holding the lock
    slicesToDelete.clear();
}

void FileBackedTimeBasedSliceStore::deleteState()
{
    DefaultTimeBasedSliceStore::deleteState();
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    slicesInMemoryLocked->clear();
    alteredSlicesPerThread.clear();
    // TODO delete memCtrl and state from ssd if there is any
}

void FileBackedTimeBasedSliceStore::setWorkerThreads(const uint64_t numberOfWorkerThreads)
{
    this->numberOfWorkerThreads = numberOfWorkerThreads;

    /// Initialise maps to keep track of altered slices
    for (auto i = 0UL; i < numberOfWorkerThreads; ++i)
    {
        alteredSlicesPerThread[{WorkerThreadId(i), JoinBuildSideType::Left}];
        alteredSlicesPerThread[{WorkerThreadId(i), JoinBuildSideType::Right}];
    }

    /// Initialise memory controller and measure execution times for reading and writing
    memoryController = std::make_shared<MemoryController>(
        sliceStoreInfo.fileDescriptorBufferSize,
        numberOfWorkerThreads,
        memoryControllerInfo.workingDir,
        memoryControllerInfo.queryId,
        memoryControllerInfo.outputOriginId);
    measureReadAndWriteExecTimes(USE_TEST_DATA_SIZES);
}

boost::asio::awaitable<void> FileBackedTimeBasedSliceStore::updateSlices(
    boost::asio::io_context& ioCtx,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const JoinBuildSideType joinBuildSide,
    const UpdateSlicesMetaData metaData)
{
    const auto& [watermark, seqNumber, originId] = metaData.bufferMetaData;
    watermarkProcessor->updateWatermark(watermark, seqNumber, originId);
    if (isExponential(++watermarkPredictorUpdateCnt[originId]))
    {
        updateWatermarkPredictor(originId);
    }

    /// Write and read all selected slices to and from disk
    const auto threadId = WorkerThreadId(metaData.threadId % numberOfWorkerThreads);
    for (const auto& [slice, operation, fileLayout] : getSlicesToUpdate(memoryLayout, joinBuildSide, threadId))
    {
        /// Prevent other threads from combining pagedVectors to preserve data integrity as pagedVectors are not thread-safe
        const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
        nljSlice->acquireCombinePagedVectorsLock();

        /// If the pagedVectors have been combined then the slice was already emitted to probe and is being joined momentarily
        if (!nljSlice->pagedVectorsCombined())
        {
            const auto sliceEnd = slice->getSliceEnd();
            auto* const pagedVector = nljSlice->getPagedVectorRef(joinBuildSide, threadId);
            switch (operation)
            {
                case FileOperation::READ: {
                    // TODO investigate why numTuples and numPages can be zero
                    if (pagedVector->getNumberOfTuplesOnDisk() > 0)
                    {
                        auto fileReader = memoryController->getFileReader(sliceEnd, threadId, joinBuildSide);
                        pagedVector->readFromFile(bufferProvider, memoryLayout, *fileReader, fileLayout);
                        memoryController->deleteFileLayout(sliceEnd, WorkerThreadId(threadId), joinBuildSide);
                    }
                    // TODO handle wrong predictions
                    break;
                }
                case FileOperation::WRITE: {
                    if (pagedVector->getNumberOfPages() > 0)
                    {
                        const auto slicesInMemoryLocked = slicesInMemory.wlock();
                        (*slicesInMemoryLocked)[{slice->getSliceEnd(), joinBuildSide}] = false;

                        const auto fileWriter = memoryController->getFileWriter(sliceEnd, threadId, joinBuildSide, ioCtx);
                        co_await pagedVector->writeToFile(bufferProvider, memoryLayout, fileWriter, fileLayout);
                        /// We need to flush and deallocate buffers now as we cannot do it from probe and we might not get this fileWriter in build again
                        co_await fileWriter->flush();
                        fileWriter->deallocateBuffers();
                        pagedVector->truncate(fileLayout);
                    }
                    // TODO handle wrong predictions
                    break;
                }
            }
        }

        nljSlice->releaseCombinePagedVectorsLock();
    }
    // TODO can we also already read back slices (left and right) as a whole? probably not because other threads might still be writing to them
    co_return;
}

std::vector<std::tuple<std::shared_ptr<Slice>, FileOperation, FileLayout>> FileBackedTimeBasedSliceStore::getSlicesToUpdate(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout, const JoinBuildSideType joinBuildSide, const WorkerThreadId threadId)
{
    std::vector<std::tuple<std::shared_ptr<Slice>, FileOperation, FileLayout>> slicesToUpdate;
    for (const auto& slice : alteredSlicesPerThread[{threadId, joinBuildSide}])
    {
        // TODO state sizes do not include size of variable sized data
        const auto sliceEnd = slice->getSliceEnd();
        const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
        const auto stateSizeOnDisk = nljSlice->getStateSizeOnDiskForThreadId(memoryLayout, joinBuildSide, threadId);
        const auto stateSizeInMemory = nljSlice->getStateSizeInMemoryForThreadId(memoryLayout, joinBuildSide, threadId);

        const auto now = std::chrono::high_resolution_clock::now();
        const auto timeNow = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());

        if (stateSizeOnDisk > sliceStoreInfo.minReadStateSize
            && AbstractWatermarkPredictor::getMinPredictedWatermarkForTimestamp(
                   watermarkPredictors,
                   timeNow + getExecTimesForDataSize(readExecTimeFunction, stateSizeOnDisk) + sliceStoreInfo.fileOperationTimeDelta)
                >= sliceEnd)
        {
            /// Slice should be read back now as it will be triggered once the read operation has finished
            if (const auto fileLayout = memoryController->getFileLayout(sliceEnd, threadId, joinBuildSide); fileLayout.has_value())
            {
                slicesToUpdate.emplace_back(slice, FileOperation::READ, fileLayout.value());
            }
            /// Slice is already being read back if no FileLayout was found
        }
        else if (
            stateSizeInMemory > sliceStoreInfo.minWriteStateSize
            && AbstractWatermarkPredictor::getMinPredictedWatermarkForTimestamp(
                   watermarkPredictors,
                   timeNow + getExecTimesForDataSize(writeExecTimeFunction, stateSizeInMemory)
                       + getExecTimesForDataSize(readExecTimeFunction, stateSizeInMemory + stateSizeOnDisk)
                       + sliceStoreInfo.fileOperationTimeDelta)
                < sliceEnd)
        {
            /// Slice should be written out as it will not be triggered before write and read operations have finished
            memoryController->setFileLayout(sliceEnd, threadId, joinBuildSide, sliceStoreInfo.fileLayout);
            slicesToUpdate.emplace_back(slice, FileOperation::WRITE, sliceStoreInfo.fileLayout);
        }
        /// Slice should not be written out or read back in any other case
    }
    alteredSlicesPerThread[{threadId, joinBuildSide}].clear();
    return slicesToUpdate;
}

void FileBackedTimeBasedSliceStore::readSliceFromFiles(
    const std::shared_ptr<Slice>& slice,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const JoinBuildSideType joinBuildSide)
{
    // TODO use rlock and conditions (-map)
    const auto sliceEnd = slice->getSliceEnd();
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    if ((*slicesInMemoryLocked)[{sliceEnd, joinBuildSide}])
    {
        /// Slice has already been read from file for this build side
        return;
    }

    /// Read files in order by WorkerThreadId as all FileBackedPagedVectors have already been combined
    for (auto threadId = 0UL; threadId < numberOfWorkerThreads; ++threadId)
    {
        /// Only read from file if the slice was written out earlier for this build side
        if (auto fileReader = memoryController->getFileReader(sliceEnd, WorkerThreadId(threadId), joinBuildSide))
        {
            const auto fileLayout = memoryController->getFileLayout(sliceEnd, WorkerThreadId(threadId), joinBuildSide);
            auto* const pagedVector
                = std::dynamic_pointer_cast<NLJSlice>(slice)->getPagedVectorRef(joinBuildSide, WorkerThreadId(threadId));
            pagedVector->readFromFile(bufferProvider, memoryLayout, *fileReader, fileLayout.value());
            memoryController->deleteFileLayout(sliceEnd, WorkerThreadId(threadId), joinBuildSide);
        }
    }
    (*slicesInMemoryLocked)[{sliceEnd, joinBuildSide}] = true;
}

void FileBackedTimeBasedSliceStore::updateWatermarkPredictor(const OriginId originId)
{
    const auto& ingestionTimesForWatermarks = watermarkProcessor->getIngestionTimesForWatermarks(
        originId, sliceStoreInfo.numWatermarkGapsAllowed, sliceStoreInfo.maxNumSequenceNumbers);
    watermarkPredictors[originId]->update(ingestionTimesForWatermarks);
}

void FileBackedTimeBasedSliceStore::measureReadAndWriteExecTimes(const std::array<size_t, USE_TEST_DATA_SIZES.size()>& dataSizes)
{
    constexpr auto numElements = USE_TEST_DATA_SIZES.size();
    if constexpr (numElements < 2)
    {
        throw std::invalid_argument("At least two points are required to initialize the model.");
    }

    boost::asio::io_context ioCtx;
    auto guard = boost::asio::make_work_guard(ioCtx);
    double sumXWrite = 0, sumYWrite = 0, sumXYWrite = 0, sumX2Write = 0;
    double sumXRead = 0, sumYRead = 0, sumXYRead = 0, sumX2Read = 0;
    for (const auto dataSize : dataSizes)
    {
        std::vector<char> data(dataSize);
        const auto start = std::chrono::high_resolution_clock::now();

        {
            /// FileWriter should be destroyed when calling getFileReader
            const auto fileWriter = memoryController->getFileWriter(
                SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(numberOfWorkerThreads), JoinBuildSideType::Left, ioCtx);
            runSingleAwaitable(ioCtx, fileWriter->write(data.data(), dataSize));
            runSingleAwaitable(ioCtx, fileWriter->flush());
        }
        const auto write = std::chrono::high_resolution_clock::now();

        const auto sizeRead
            = memoryController
                  ->getFileReader(SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(numberOfWorkerThreads), JoinBuildSideType::Left)
                  ->read(data.data(), dataSize);
        const auto read = std::chrono::high_resolution_clock::now();

        if (sizeRead != dataSize)
        {
            throw std::runtime_error("Data does not match");
        }

        const auto startTicks
            = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(start.time_since_epoch()).count());
        const auto writeTicks
            = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(write.time_since_epoch()).count());
        const auto readTicks
            = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(read.time_since_epoch()).count());

        const auto writeExecTime = writeTicks - startTicks;
        sumXWrite += dataSize;
        sumYWrite += writeExecTime;
        sumXYWrite += dataSize * writeExecTime;
        sumX2Write += dataSize * dataSize;
        const auto readExecTimes = readTicks - writeTicks;
        sumXRead += dataSize;
        sumYRead += readExecTimes;
        sumXYRead += dataSize * readExecTimes;
        sumX2Read += dataSize * dataSize;
    }
    guard.reset();

    const auto writeSlope = (numElements * sumXYWrite - sumXWrite * sumYWrite) / (numElements * sumX2Write - sumXWrite * sumXWrite);
    const auto writeIntercept = (sumYWrite - writeSlope * sumXWrite) / numElements;
    writeExecTimeFunction = {writeSlope, writeIntercept};

    const auto readSlope = (numElements * sumXYRead - sumXRead * sumYRead) / (numElements * sumX2Read - sumXRead * sumXRead);
    const auto readIntercept = (sumYRead - readSlope * sumXRead) / numElements;
    readExecTimeFunction = {readSlope, readIntercept};
}

}
