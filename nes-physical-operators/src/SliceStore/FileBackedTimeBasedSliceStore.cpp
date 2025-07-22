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

#include <ranges>
#include <SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <SliceStore/WatermarkPredictor/KalmanBasedWindowTriggerPredictor.hpp>
#include <SliceStore/WatermarkPredictor/RLSBasedWatermarkPredictor.hpp>
#include <SliceStore/WatermarkPredictor/RegressionBasedWatermarkPredictor.hpp>

namespace NES
{
FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(
    const uint64_t windowSize,
    const uint64_t windowSlide,
    SliceStoreInfo sliceStoreInfo,
    FileDescriptorManagerInfo fileDescriptorManagerInfo,
    const WatermarkPredictorType watermarkPredictorType,
    const std::vector<OriginId>& inputOrigins)
    : DefaultTimeBasedSliceStore(windowSize, windowSlide)
    , watermarkProcessor(std::make_shared<MultiOriginWatermarkProcessor>(inputOrigins))
    , numberOfWorkerThreads(0)
    , sliceStoreInfo(std::move(sliceStoreInfo))
    , fileDescriptorManagerInfo(std::move(fileDescriptorManagerInfo))
{
    if (this->sliceStoreInfo.lowerMemoryBound > this->sliceStoreInfo.upperMemoryBound)
    {
        throw std::runtime_error("lowerMemoryBound cannot be larger than upperMemoryBound");
    }

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

FileBackedTimeBasedSliceStore::~FileBackedTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide,
    const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    const auto& slicesVec = DefaultTimeBasedSliceStore::getSlicesOrCreate(timestamp, threadId, joinBuildSide, createNewSlice);
    for (const auto& slice : slicesVec)
    {
        alteredSlicesPerThread[{WorkerThreadId(threadId % numberOfWorkerThreads), joinBuildSide}].emplace_back(slice);
    }
    return slicesVec;
}

std::optional<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSliceBySliceEnd(
    const SliceEnd sliceEnd,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide)
{
    const auto& slice = DefaultTimeBasedSliceStore::getSliceBySliceEnd(sliceEnd, bufferProvider, memoryLayout, threadId, joinBuildSide);
    if (slice.has_value())
    {
        readSliceFromFiles(
            slice.value(),
            bufferProvider,
            memoryLayout,
            [this]
            {
                std::vector<WorkerThreadId> allThreadIds;
                allThreadIds.reserve(numberOfWorkerThreads);
                std::ranges::for_each(
                    std::views::iota(0UL, numberOfWorkerThreads), [&allThreadIds](uint64_t id) { allThreadIds.emplace_back(id); });
                return allThreadIds;
            }(),
            WorkerThreadId(threadId % numberOfWorkerThreads),
            joinBuildSide);
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

    /// Now we can remove/call destructor on every slice without still holding the lock
    for (const auto& slice : slicesToDelete)
    {
        fileDescriptorManager->deleteFileWriters(slice->getSliceEnd(), sliceStoreInfo.withCleanup);
    }
    slicesToDelete.clear();
}

void FileBackedTimeBasedSliceStore::deleteState()
{
    DefaultTimeBasedSliceStore::deleteState();
    if (sliceStoreInfo.withCleanup)
    {
        fileDescriptorManager->deleteAllSliceFiles();
    }
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
    /// Separate keys means keys and payload are written to separate files, and we may need an additional descriptor for variable sized data
    /// We then also need two buffers, one for key and one for payload file
    const auto minNumFileDescriptorsPerWorker = sliceStoreInfo.fileLayout == FileLayout::SEPARATE_KEYS ? 3UL : 2UL;
    const auto memoryPoolSizeMultiplier = sliceStoreInfo.fileLayout == FileLayout::SEPARATE_KEYS ? 2UL : 1UL;
    fileDescriptorManager = std::make_shared<FileDescriptorManager>(
        fileDescriptorManagerInfo, numberOfWorkerThreads, minNumFileDescriptorsPerWorker, memoryPoolSizeMultiplier);
    measureReadAndWriteExecTimes(TEST_DATA_SIZES);
}

boost::asio::awaitable<void> FileBackedTimeBasedSliceStore::updateSlices(
    boost::asio::io_context& ioCtx,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const BufferMetaData& bufferMetaData,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide)
{
    const auto& [timestamp, seqNumber, originId] = bufferMetaData;
    const auto watermark
        = sliceStoreInfo.upperMemoryBound == 0 ? Timestamp(0) : watermarkProcessor->updateWatermark(timestamp, seqNumber, originId);
    if (sliceStoreInfo.withPrediction and isExponential(++watermarkPredictorUpdateCnt[originId]))
    {
        updateWatermarkPredictor(originId);
    }

    /// Write and read all selected slices to and from disk
    const auto workerThreadId = WorkerThreadId(threadId % numberOfWorkerThreads);
    //std::cout << fmt::format("Updating slices for thread {}\n", threadId.getRawValue());
    for (auto&& [slice, operation] : getSlicesToUpdate(bufferProvider, memoryLayout, watermark, workerThreadId, joinBuildSide))
    {
        switch (operation)
        {
            case FileOperation::READ: {
                readSliceFromFiles(slice, bufferProvider, memoryLayout, {workerThreadId}, workerThreadId, joinBuildSide);
                break;
            }
            case FileOperation::WRITE: {
                co_await writeSliceToFile(ioCtx, slice, bufferProvider, memoryLayout, workerThreadId, joinBuildSide);
                break;
            }
        }
    }
    // TODO can we also already read back slices (left and right) as a whole? probably not because other threads might still be writing to them
}

std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> FileBackedTimeBasedSliceStore::getSlicesToUpdate(
    const Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const Timestamp watermark,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide)
{
    if (sliceStoreInfo.upperMemoryBound == 0)
    {
        return updateSlicesReactive(threadId, joinBuildSide);
    }

    const auto totalSizeOfUsedBuffers
        = (bufferProvider->getNumOfPooledBuffers() - bufferProvider->getAvailableBuffers()) * bufferProvider->getBufferSize()
        + bufferProvider->getTotalSizeOfUnpooledBufferChunks();
    if (totalSizeOfUsedBuffers >= sliceStoreInfo.upperMemoryBound)
    {
        return updateSlicesReactive(threadId, joinBuildSide);
    }
    if (totalSizeOfUsedBuffers >= sliceStoreInfo.lowerMemoryBound)
    {
        return sliceStoreInfo.withPrediction ? updateSlicesProactiveWithPrediction(memoryLayout, threadId, joinBuildSide)
                                             : updateSlicesProactiveWithoutPrediction(watermark, threadId, joinBuildSide);
    }

    return std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>>{};
}

std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>>
FileBackedTimeBasedSliceStore::updateSlicesReactive(const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    /// Reserve space for every altered slice as we update all of them
    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> slicesToUpdate;
    slicesToUpdate.reserve(alteredSlicesPerThread[{threadId, joinBuildSide}].size());

    std::ranges::transform(
        alteredSlicesPerThread[{threadId, joinBuildSide}],
        std::back_inserter(slicesToUpdate),
        [](const std::shared_ptr<Slice>& slice) { return std::make_pair(slice, FileOperation::WRITE); });

    alteredSlicesPerThread[{threadId, joinBuildSide}].clear();
    return slicesToUpdate;
}

std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> FileBackedTimeBasedSliceStore::updateSlicesProactiveWithoutPrediction(
    const Timestamp watermark, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    /// Reserve space for every altered slice as we update all of them
    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> slicesToUpdate;
    slicesToUpdate.reserve(alteredSlicesPerThread[{threadId, joinBuildSide}].size());

    std::ranges::transform(
        alteredSlicesPerThread[{threadId, joinBuildSide}],
        std::back_inserter(slicesToUpdate),
        [watermark](const std::shared_ptr<Slice>& slice)
        {
            if (watermark < slice->getSliceEnd()) [[likely]]
            {
                return std::make_pair(slice, FileOperation::WRITE);
            }
            return std::make_pair(slice, FileOperation::READ);
        });

    alteredSlicesPerThread[{threadId, joinBuildSide}].clear();
    return slicesToUpdate;
}

std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> FileBackedTimeBasedSliceStore::updateSlicesProactiveWithPrediction(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    /// Reserve space for every altered slice as we might update all of them
    std::vector<std::pair<std::shared_ptr<Slice>, FileOperation>> slicesToUpdate;
    slicesToUpdate.reserve(alteredSlicesPerThread[{threadId, joinBuildSide}].size());

    auto tupleSize = memoryLayout->getTupleSize();
    if (sliceStoreInfo.fileLayout == FileLayout::SEPARATE_PAYLOAD)
    {
        tupleSize -= memoryLayout->createKeyFieldsOnlySchema().getSizeOfSchemaInBytes();
    }

    for (auto&& slice : alteredSlicesPerThread[{threadId, joinBuildSide}])
    {
        // TODO state sizes do not include size of variable sized data
        const auto sliceEnd = slice->getSliceEnd();
        const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
        const auto *const pagedVector = nljSlice->getPagedVectorRef(threadId, joinBuildSide);

        auto stateSizeOnDisk = 0UL;
        auto stateSizeInMemory = 0UL;
        nljSlice->acquireCombinePagedVectorsLock();
        if (not nljSlice->pagedVectorsCombined())
        {
            stateSizeOnDisk = pagedVector->getNumberOfTuplesOnDisk();
            stateSizeInMemory = pagedVector->getNumberOfEntries();
        }
        nljSlice->releaseCombinePagedVectorsLock();
        stateSizeOnDisk *= tupleSize;
        stateSizeInMemory *= tupleSize;

        const auto now = std::chrono::high_resolution_clock::now();
        const auto timeNow = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());

        if (stateSizeOnDisk > sliceStoreInfo.minReadStateSize
            && AbstractWatermarkPredictor::getMinPredictedWatermarkForTimestamp(
                   watermarkPredictors,
                   timeNow + getExecTimesForDataSize(readExecTimeFunction, stateSizeOnDisk) + sliceStoreInfo.predictionTimeDelta)
                >= sliceEnd)
        {
            /// Slice should be read back now as it will be triggered once the read operation has finished
            slicesToUpdate.emplace_back(slice, FileOperation::READ);
        }
        else if (
            stateSizeInMemory > sliceStoreInfo.minWriteStateSize
            && AbstractWatermarkPredictor::getMinPredictedWatermarkForTimestamp(
                   watermarkPredictors,
                   timeNow + getExecTimesForDataSize(writeExecTimeFunction, stateSizeInMemory)
                       + getExecTimesForDataSize(readExecTimeFunction, stateSizeInMemory + stateSizeOnDisk)
                       + sliceStoreInfo.predictionTimeDelta)
                < sliceEnd)
        {
            /// Slice should be written out as it will not be triggered before write and read operations have finished
            slicesToUpdate.emplace_back(slice, FileOperation::WRITE);
        }
        /// Slice should not be written out or read back in any other case
    }

    alteredSlicesPerThread[{threadId, joinBuildSide}].clear();
    return slicesToUpdate;
}

boost::asio::awaitable<void> FileBackedTimeBasedSliceStore::writeSliceToFile(
    boost::asio::io_context& ioCtx,
    const std::shared_ptr<Slice>& slice,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const WorkerThreadId threadId,
    const JoinBuildSideType joinBuildSide) const
{
    const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
    const auto fileWriter = fileDescriptorManager->getFileWriter(ioCtx, nljSlice->getSliceEnd(), threadId, joinBuildSide);
    auto* const pagedVector = nljSlice->getPagedVectorRef(threadId, joinBuildSide);

    /// Prevent other threads from combining pagedVectors to preserve data integrity as pagedVectors are not thread-safe
    nljSlice->acquireCombinePagedVectorsLock();

    /// Check if there are tuples to offload
    if (not nljSlice->pagedVectorsCombined() and pagedVector->getNumberOfPages() > 0)
    {
        co_await pagedVector->writeToFile(bufferProvider, memoryLayout, fileWriter, sliceStoreInfo.fileLayout);
        pagedVector->truncate(sliceStoreInfo.fileLayout);
    }
    nljSlice->releaseCombinePagedVectorsLock();

    // TODO handle wrong predictions
}

void FileBackedTimeBasedSliceStore::readSliceFromFiles(
    const std::shared_ptr<Slice>& slice,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const std::vector<WorkerThreadId>& threadsToRead,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType joinBuildSide) const
{
    const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);

    /// Read files in order by WorkerThreadId as all FileBackedPagedVectors have already been combined
    for (const auto threadToRead : threadsToRead)
    {
        /// Only read from file if the slice was written out earlier for this build side and not yet read back
        if (auto fileReader = fileDescriptorManager->getFileReader(
                nljSlice->getSliceEnd(), threadToRead, workerThreadId, joinBuildSide, sliceStoreInfo.withCleanup);
            fileReader.has_value())
        {
            auto* const pagedVector = nljSlice->getPagedVectorRef(threadToRead, joinBuildSide);
            nljSlice->acquireCombinePagedVectorsLock();
            if (pagedVector->getNumberOfTuplesOnDisk() > 0)
            {
                pagedVector->readFromFile(bufferProvider, memoryLayout, fileReader.value(), sliceStoreInfo.fileLayout);
            }
            nljSlice->releaseCombinePagedVectorsLock();
        }
    }
}

void FileBackedTimeBasedSliceStore::updateWatermarkPredictor(const OriginId originId)
{
    const auto& ingestionTimesForWatermarks = watermarkProcessor->getIngestionTimesForWatermarks(
        originId, sliceStoreInfo.maxNumWatermarkGaps, sliceStoreInfo.maxNumSequenceNumbers);
    watermarkPredictors[originId]->update(ingestionTimesForWatermarks);
}

void FileBackedTimeBasedSliceStore::measureReadAndWriteExecTimes(const std::array<size_t, TEST_DATA_SIZES.size()>& dataSizes)
{
    constexpr auto numElements = TEST_DATA_SIZES.size();
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
            const auto fileWriter = fileDescriptorManager->getFileWriter(
                ioCtx, SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(0), JoinBuildSideType::Left);
            runSingleAwaitable(ioCtx, fileWriter->write(data.data(), dataSize));
        }
        const auto write = std::chrono::high_resolution_clock::now();

        const auto sizeRead = fileDescriptorManager
                                  ->getFileReader(
                                      SliceEnd(SliceEnd::INVALID_VALUE),
                                      WorkerThreadId(0),
                                      WorkerThreadId(0),
                                      JoinBuildSideType::Left,
                                      sliceStoreInfo.withCleanup)
                                  .value()
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
