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
#include <Execution/Operators/SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WatermarkPredictor/KalmanBasedWindowTriggerPredictor.hpp>
#include <Execution/Operators/SliceStore/WatermarkPredictor/RLSBasedWatermarkPredictor.hpp>
#include <Execution/Operators/SliceStore/WatermarkPredictor/RegressionBasedWatermarkPredictor.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/Locks.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution
{
FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(
    const uint64_t windowSize,
    const uint64_t windowSlide,
    MemoryControllerInfo memoryControllerInfo,
    const WatermarkPredictorType watermarkPredictorType,
    const std::vector<OriginId>& inputOrigins)
    : DefaultTimeBasedSliceStore(windowSize, windowSlide, inputOrigins.size())
    , watermarkProcessor(std::make_shared<Operators::MultiOriginWatermarkProcessor>(inputOrigins))
    , numberOfWorkerThreads(0)
    , memoryControllerInfo(std::move(memoryControllerInfo))
{
    for (const auto origin : inputOrigins)
    {
        watermarkPredictorUpdateCnt.emplace(origin, 0);
        switch (watermarkPredictorType)
        {
            case KalmanBased: {
                watermarkPredictors.emplace(origin, std::make_shared<KalmanWindowTriggerPredictor>());
                break;
            }
            case RegressionBased: {
                watermarkPredictors.emplace(origin, std::make_shared<RegressionBasedWatermarkPredictor>());
                break;
            }
            case RLSBased: {
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
    numberOfWorkerThreads = other.numberOfWorkerThreads;
    memoryControllerInfo = other.memoryControllerInfo;
    return *this;
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore&& other) noexcept
{
    DefaultTimeBasedSliceStore::operator=(other);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesInMemoryLocked = std::move(*otherSlicesInMemoryLocked);

    watermarkProcessor = std::move(other.watermarkProcessor);
    watermarkPredictors = std::move(other.watermarkPredictors);
    writeExecTimeFunction = std::move(other.writeExecTimeFunction);
    readExecTimeFunction = std::move(other.readExecTimeFunction);
    memoryController = std::move(other.memoryController);
    alteredSlicesPerThread = std::move(other.alteredSlicesPerThread);
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
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    const auto threadId = WorkerThreadId(workerThreadId % numberOfWorkerThreads);
    const auto& slicesVec = DefaultTimeBasedSliceStore::getSlicesOrCreate(timestamp, workerThreadId, joinBuildSide, createNewSlice);
    for (const auto& slice : slicesVec)
    {
        alteredSlicesPerThread[{threadId, joinBuildSide}].emplace_back(slice);
        const auto slicesInMemoryLocked = slicesInMemory.wlock();
        slicesInMemoryLocked->insert({{slice->getSliceEnd(), QueryCompilation::JoinBuildSideType::Left}, true});
        slicesInMemoryLocked->insert({{slice->getSliceEnd(), QueryCompilation::JoinBuildSideType::Right}, true});
    }
    return slicesVec;
}

std::optional<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSliceBySliceEnd(
    const SliceEnd sliceEnd,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide)
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
    auto lockedSlicesAndWindows = tryAcquireLocked(slices, windows);
    if (not lockedSlicesAndWindows)
    {
        /// We could not acquire the lock, so we opt for not performing the garbage collection this time.
        return;
    }
    auto& [slicesWriteLocked, windowsWriteLocked] = *lockedSlicesAndWindows;

    NES_TRACE("Performing garbage collection for new global watermark {}", newGlobalWaterMark);

    /// 1. We iterate over all windows and erase them if they can be deleted
    /// This condition is true, if the window end is smaller than the new global watermark of the probe phase.
    for (auto windowsLockedIt = windowsWriteLocked->cbegin(); windowsLockedIt != windowsWriteLocked->cend();)
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

    /// 2. We gather all slices if they are not used in any window that has not been triggered/can not be deleted yet
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    for (auto slicesLockedIt = slicesWriteLocked->begin(); slicesLockedIt != slicesWriteLocked->end();)
    {
        const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
        if (sliceEnd + sliceAssigner.getWindowSize() < newGlobalWaterMark)
        {
            NES_TRACE("Deleting slice with sliceEnd {} as it is not used anymore", sliceEnd);
            memoryController->deleteSliceFiles(sliceEnd);
            slicesInMemoryLocked->erase({sliceEnd, QueryCompilation::JoinBuildSideType::Left});
            slicesInMemoryLocked->erase({sliceEnd, QueryCompilation::JoinBuildSideType::Right});
            slicesLockedIt = slicesWriteLocked->erase(slicesLockedIt);
        }
        else
        {
            /// As the slices are sorted (due to std::map), we can break here as we will not find any slices with a smaller slice end
            break;
        }
    }
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
        alteredSlicesPerThread[{WorkerThreadId(i), QueryCompilation::JoinBuildSideType::Left}];
        alteredSlicesPerThread[{WorkerThreadId(i), QueryCompilation::JoinBuildSideType::Right}];
    }

    /// Initialise memory controller and measure execution times for reading and writing
    memoryController = std::make_shared<MemoryController>(
        USE_BUFFER_SIZE,
        numberOfWorkerThreads,
        USE_NUM_WRITE_BUFFERS,
        numberOfWorkerThreads,
        memoryControllerInfo.workingDir,
        memoryControllerInfo.queryId,
        memoryControllerInfo.outputOriginId);
    measureReadAndWriteExecTimes(USE_TEST_DATA_SIZES);
}

void FileBackedTimeBasedSliceStore::updateSlices(
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const SliceStoreMetaData& metaData)
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
                case READ: {
                    // TODO investigate why numTuples and numPages can be zero
                    if (const auto numTuplesOnDisk = pagedVector->getNumberOfTuplesOnDisk(); numTuplesOnDisk > 0)
                    {
                        auto fileReader = memoryController->getFileReader(sliceEnd, threadId, joinBuildSide);
                        pagedVector->readFromFile(bufferProvider, memoryLayout, *fileReader, fileLayout);
                        memoryController->deleteFileLayout(sliceEnd, WorkerThreadId(threadId), joinBuildSide);
                    }
                    break;
                }
                case WRITE: {
                    if (const auto numPagesInMemory = pagedVector->getNumberOfPages(); numPagesInMemory > 0)
                    {
                        const auto slicesInMemoryLocked = slicesInMemory.wlock();
                        (*slicesInMemoryLocked)[{sliceEnd, joinBuildSide}] = false;

                        auto fileWriter = memoryController->getFileWriter(sliceEnd, threadId, joinBuildSide);
                        pagedVector->writeToFile(bufferProvider, memoryLayout, *fileWriter, fileLayout);
                        pagedVector->truncate(fileLayout);
                        // TODO force flush FileWriter?
                        fileWriter->flushAndDeallocateBuffer();
                    }
                    break;
                }
            }
        }

        nljSlice->releaseCombinePagedVectorsLock();
    }
    // TODO can we also already read back slices (left and right) as a whole? probably not because other threads might still be writing to them
}

std::vector<std::tuple<std::shared_ptr<Slice>, DiskOperation, FileLayout>> FileBackedTimeBasedSliceStore::getSlicesToUpdate(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const WorkerThreadId threadId)
{
    std::vector<std::tuple<std::shared_ptr<Slice>, DiskOperation, FileLayout>> slicesToUpdate;
    for (const auto& slice : alteredSlicesPerThread[{threadId, joinBuildSide}])
    {
        // TODO state sizes do not include size of variable sized data
        const auto sliceEnd = slice->getSliceEnd();
        const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
        const auto stateSizeOnDisk = nljSlice->getStateSizeOnDiskForThreadId(memoryLayout, joinBuildSide, threadId);
        const auto stateSizeInMemory = nljSlice->getStateSizeInMemoryForThreadId(memoryLayout, joinBuildSide, threadId);

        const auto now = std::chrono::high_resolution_clock::now();
        const auto timeNow = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());

        if (stateSizeOnDisk > USE_MIN_STATE_SIZE_READ
            && AbstractWatermarkPredictor::getMinPredictedWatermarkForTimestamp(
                   watermarkPredictors, timeNow + getExecTimesForDataSize(readExecTimeFunction, stateSizeOnDisk) + USE_TIME_DELTA_MS)
                >= sliceEnd)
        {
            /// Slice should be read back now as it will be triggered once the read operation has finished
            if (const auto fileLayout = memoryController->getFileLayout(sliceEnd, threadId, joinBuildSide); fileLayout.has_value())
            {
                slicesToUpdate.emplace_back(slice, READ, fileLayout.value());
            }
            /// Slice is already being read back if no FileLayout was found
        }
        else if (
            stateSizeInMemory > USE_MIN_STATE_SIZE_WRITE
            && AbstractWatermarkPredictor::getMinPredictedWatermarkForTimestamp(
                   watermarkPredictors,
                   timeNow + getExecTimesForDataSize(writeExecTimeFunction, stateSizeInMemory)
                       + getExecTimesForDataSize(readExecTimeFunction, stateSizeInMemory + stateSizeOnDisk) + USE_TIME_DELTA_MS)
                < sliceEnd)
        {
            /// Slice should be written out as it will not be triggered before write and read operations have finished
            memoryController->setFileLayout(sliceEnd, threadId, joinBuildSide, USE_FILE_LAYOUT);
            slicesToUpdate.emplace_back(slice, WRITE, USE_FILE_LAYOUT);
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
    const QueryCompilation::JoinBuildSideType joinBuildSide)
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
    const auto& ingestionTimesForWatermarks
        = watermarkProcessor->getIngestionTimesForWatermarks(originId, USE_NUM_GAPS_ALLOWED, USE_MAX_NUM_SEQ_NUMBERS);
    watermarkPredictors[originId]->update(ingestionTimesForWatermarks);
}

void FileBackedTimeBasedSliceStore::measureReadAndWriteExecTimes(const std::array<size_t, USE_TEST_DATA_SIZES.size()>& dataSizes)
{
    constexpr auto numElements = USE_TEST_DATA_SIZES.size();
    if constexpr (numElements < 2)
    {
        throw std::invalid_argument("At least two points are required to initialize the model.");
    }

    double sumXWrite = 0, sumYWrite = 0, sumXYWrite = 0, sumX2Write = 0;
    double sumXRead = 0, sumYRead = 0, sumXYRead = 0, sumX2Read = 0;
    for (const auto dataSize : dataSizes)
    {
        std::vector<char> data(dataSize);
        const auto start = std::chrono::high_resolution_clock::now();

        const auto fileWriter = memoryController->getFileWriter(
            SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(numberOfWorkerThreads), QueryCompilation::JoinBuildSideType::Left);
        fileWriter->write(data.data(), dataSize);
        const auto write = std::chrono::high_resolution_clock::now();

        const auto fileReader = memoryController->getFileReader(
            SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(numberOfWorkerThreads), QueryCompilation::JoinBuildSideType::Left);
        fileReader->read(data.data(), dataSize);
        const auto read = std::chrono::high_resolution_clock::now();

        const auto startTicks
            = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(start.time_since_epoch()).count());
        const auto writeTicks
            = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(write.time_since_epoch()).count());
        const auto readTicks
            = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(read.time_since_epoch()).count());

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

    const auto writeSlope = (numElements * sumXYWrite - sumXWrite * sumYWrite) / (numElements * sumX2Write - sumXWrite * sumXWrite);
    const auto writeIntercept = (sumYWrite - writeSlope * sumXWrite) / numElements;
    writeExecTimeFunction = {writeSlope, writeIntercept};

    const auto readSlope = (numElements * sumXYRead - sumXRead * sumYRead) / (numElements * sumX2Read - sumXRead * sumXRead);
    const auto readIntercept = (sumYRead - readSlope * sumXRead) / numElements;
    readExecTimeFunction = {readSlope, readIntercept};
}

}
