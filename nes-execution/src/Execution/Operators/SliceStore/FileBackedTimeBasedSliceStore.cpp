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
#include <Execution/Operators/SliceStore/WatermarkPredictor/RegressionBasedWatermarkPredictor.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
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
    const WatermarkPredictorMetaData predictorMetaData,
    const std::vector<OriginId>& inputOrigins,
    const std::filesystem::path& workingDir,
    const QueryId queryId,
    const OriginId originId)
    : memCtrl(USE_BUFFER_SIZE, USE_POOL_SIZE, workingDir, queryId, originId)
    , watermarkProcessor(std::make_shared<Operators::MultiOriginWatermarkProcessor>(inputOrigins))
    , sliceAssigner(windowSize, windowSlide)
    , sequenceNumber(SequenceNumber::INITIAL)
    , numberOfActiveOrigins(inputOrigins.size())
{
    for (const auto origin : inputOrigins)
    {
        switch (predictorMetaData.type)
        {
            case RegressionBased: {
                watermarkPredictors.emplace(origin, std::make_unique<RegressionBasedWatermarkPredictor>(predictorMetaData.param));
                break;
            }
        }
    }

    measureReadAndWriteExecTimes(USE_TEST_DATA_SIZES);
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore& other)
    : memCtrl(other.memCtrl)
    , watermarkProcessor(other.watermarkProcessor)
    , watermarkPredictors(other.watermarkPredictors)
    , writeExecTimes(other.writeExecTimes)
    , readExecTimes(other.readExecTimes)
    , sliceAssigner(other.sliceAssigner)
    , sequenceNumber(other.sequenceNumber.load())
    , numberOfActiveOrigins(other.numberOfActiveOrigins)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesWriteLocked = *otherSlicesWriteLocked;
    *windowsWriteLocked = *otherWindowsWriteLocked;
    *slicesInMemoryLocked = *otherSlicesInMemoryLocked;
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore&& other) noexcept
    : memCtrl(std::move(other.memCtrl))
    , watermarkProcessor(std::move(other.watermarkProcessor))
    , watermarkPredictors(std::move(other.watermarkPredictors))
    , writeExecTimes(std::move(other.writeExecTimes))
    , readExecTimes(std::move(other.readExecTimes))
    , sliceAssigner(std::move(other.sliceAssigner))
    , sequenceNumber(std::move(other.sequenceNumber.load()))
    , numberOfActiveOrigins(std::move(other.numberOfActiveOrigins))
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesWriteLocked = std::move(*otherSlicesWriteLocked);
    *windowsWriteLocked = std::move(*otherWindowsWriteLocked);
    *slicesInMemoryLocked = std::move(*otherSlicesInMemoryLocked);
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore& other)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesReadLocked, otherWindowsReadLocked] = acquireLocked(other.slices, other.windows);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesWriteLocked = *otherSlicesReadLocked;
    *windowsWriteLocked = *otherWindowsReadLocked;
    *slicesInMemoryLocked = *otherSlicesInMemoryLocked;

    memCtrl = other.memCtrl;
    watermarkProcessor = other.watermarkProcessor;
    watermarkPredictors = other.watermarkPredictors;
    writeExecTimes = other.writeExecTimes;
    readExecTimes = other.readExecTimes;
    sliceAssigner = other.sliceAssigner;
    sequenceNumber = other.sequenceNumber.load();
    numberOfActiveOrigins = other.numberOfActiveOrigins;
    return *this;
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore&& other) noexcept
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    const auto otherSlicesInMemoryLocked = other.slicesInMemory.wlock();
    *slicesWriteLocked = std::move(*otherSlicesWriteLocked);
    *windowsWriteLocked = std::move(*otherWindowsWriteLocked);
    *slicesInMemoryLocked = std::move(*otherSlicesInMemoryLocked);

    memCtrl = std::move(other.memCtrl);
    watermarkProcessor = std::move(other.watermarkProcessor);
    watermarkPredictors = std::move(other.watermarkPredictors);
    writeExecTimes = std::move(other.writeExecTimes);
    readExecTimes = std::move(other.readExecTimes);
    sliceAssigner = std::move(other.sliceAssigner);
    sequenceNumber = std::move(other.sequenceNumber.load());
    numberOfActiveOrigins = std::move(other.numberOfActiveOrigins);
    return *this;
}

std::vector<WindowInfo> FileBackedTimeBasedSliceStore::getAllWindowInfosForSlice(const Slice& slice) const
{
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart().getRawValue();
    const auto sliceEnd = slice.getSliceEnd().getRawValue();
    const auto windowSize = sliceAssigner.getWindowSize();
    const auto windowSlide = sliceAssigner.getWindowSlide();

    /// Taking the max out of sliceEnd and windowSize, allows us to not create windows, such as 0-5 for slide 5 and size 100.
    /// In our window model, a window is always the size of the window size.
    const auto firstWindowEnd = std::max(sliceEnd, windowSize);
    const auto lastWindowEnd = sliceStart + windowSize;

    for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide)
    {
        allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
    }

    return allWindows;
}

FileBackedTimeBasedSliceStore::~FileBackedTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);

    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);

    if (slicesWriteLocked->contains(sliceEnd))
    {
        return {slicesWriteLocked->find(sliceEnd)->second};
    }

    /// We assume that only one slice is created per timestamp
    const auto newSlices = createNewSlice(sliceStart, sliceEnd);
    INVARIANT(newSlices.size() == 1, "We assume that only one slice is created per timestamp for our file-backed time-based slice store.");
    auto newSlice = newSlices[0];
    slicesWriteLocked->emplace(sliceEnd, newSlice);

    /// Update the state of all windows that contain this slice as we have to expect new tuples
    for (auto windowInfo : getAllWindowInfosForSlice(*newSlice))
    {
        auto& [windowSlices, windowState] = (*windowsWriteLocked)[windowInfo];
        INVARIANT(
            windowState != WindowInfoState::EMITTED_TO_PROBE, "We should not add slices to a window that has already been triggered.");
        windowState = WindowInfoState::WINDOW_FILLING;
        windowSlices.emplace_back(newSlice);
    }

    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    slicesInMemoryLocked->insert({{newSlice->getSliceEnd(), QueryCompilation::JoinBuildSideType::Left}, true});
    slicesInMemoryLocked->insert({{newSlice->getSliceEnd(), QueryCompilation::JoinBuildSideType::Right}, true});

    return {newSlice};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
FileBackedTimeBasedSliceStore::getTriggerableWindowSlices(const Timestamp globalWatermark)
{
    /// We are iterating over all windows and check if they can be triggered
    /// A window can be triggered if both sides have been filled and the window end is smaller than the new global watermark
    const auto windowsWriteLocked = windows.wlock();
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        if (windowInfo.windowEnd >= globalWatermark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        if (windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            /// This window has already been triggered
            continue;
        }

        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
        /// As the windows are sorted, we can simply increment the sequence number here.
        const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
    }
    return windowsToSlices;
}

std::optional<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSliceBySliceEnd(
    const SliceEnd sliceEnd,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t numberOfWorkerThreads)
{
    if (const auto slicesReadLocked = slices.rlock(); slicesReadLocked->contains(sliceEnd))
    {
        auto slice = slicesReadLocked->find(sliceEnd)->second;
        readSliceFromFiles(slice, bufferProvider, memoryLayout, joinBuildSide, numberOfWorkerThreads);
        return slice;
    }
    return {};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> FileBackedTimeBasedSliceStore::getAllNonTriggeredSlices()
{
    /// Acquiring a lock for the windows, as we have to iterate over all windows and trigger all non-triggered windows
    const auto windowsWriteLocked = windows.wlock();

    /// numberOfActiveOrigins is guarded by the windows lock.
    /// If this method gets called, we know that an origin has terminated.
    INVARIANT(numberOfActiveOrigins > 0, "Method should not be called if all origin have terminated.");
    --numberOfActiveOrigins;

    /// Creating a lambda to add all slices to the return map windowsToSlices
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    auto addAllSlicesToReturnMap = [&windowsToSlices, this](const WindowInfo& windowInfo, SlicesAndState& windowSlicesAndState)
    {
        const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
    };

    /// We are iterating over all windows and check if they can be triggered
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        switch (windowSlicesAndState.windowState)
        {
            case WindowInfoState::EMITTED_TO_PROBE:
                continue;
            case WindowInfoState::WINDOW_FILLING: {
                /// If we are waiting on more than one origin to terminate, we can not trigger the window yet
                if (numberOfActiveOrigins > 0)
                {
                    windowSlicesAndState.windowState = WindowInfoState::WAITING_ON_TERMINATION;
                    NES_TRACE(
                        "Waiting on termination for window end {} and number of origins terminated {}",
                        windowInfo.windowEnd,
                        numberOfActiveOrigins);
                    break;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
            }
            case WindowInfoState::WAITING_ON_TERMINATION: {
                /// Checking if all origins have terminated (i.e., the number of origins terminated is 0, as we will decrement it during fetch_sub)
                NES_TRACE(
                    "Checking if all origins have terminated for window with window end {} and number of origins terminated {}",
                    windowInfo.windowEnd,
                    numberOfActiveOrigins);
                if (numberOfActiveOrigins > 0)
                {
                    continue;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
            }
        }
    }

    return windowsToSlices;
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
    const auto slicesInMemoryLocked = slicesInMemory.wlock();

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
    for (auto slicesLockedIt = slicesWriteLocked->cbegin(); slicesLockedIt != slicesWriteLocked->cend();)
    {
        const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
        if (sliceEnd + sliceAssigner.getWindowSize() < newGlobalWaterMark)
        {
            NES_TRACE("Deleting slice with sliceEnd {} as it is not used anymore", sliceEnd);
            memCtrl.deleteSliceFiles(sliceEnd);
            slicesInMemoryLocked->erase(slicesInMemoryLocked->find({sliceEnd, QueryCompilation::JoinBuildSideType::Left}));
            slicesInMemoryLocked->erase(slicesInMemoryLocked->find({sliceEnd, QueryCompilation::JoinBuildSideType::Right}));
            slicesWriteLocked->erase(slicesLockedIt++);
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
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    slicesWriteLocked->clear();
    windowsWriteLocked->clear();
    slicesInMemoryLocked->clear();
    // TODO delete memCtrl and state from ssd if there is any
}

uint64_t FileBackedTimeBasedSliceStore::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}

void FileBackedTimeBasedSliceStore::updateSlices(
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t numberOfWorkerThreads,
    const SliceStoreMetaData& metaData)
{
    const auto threadId = WorkerThreadId(metaData.threadId % numberOfWorkerThreads);
    const auto bufMetaData = metaData.bufferMetaData;

    // TODO don't get new ingestion times every time (time should progress linearly so ideally we would only call it once), just update it from time to time
    watermarkProcessor->updateWatermark(bufMetaData.watermarkTs, bufMetaData.seqNumber, bufMetaData.originId);
    const auto& ingestionTimesForWatermarks
        = watermarkProcessor->getIngestionTimeForWatermarks(USE_NUM_GAPS_ALLOWED, USE_MAX_NUM_SEQ_NUMBERS);
    for (const auto& [origin, predictor] : watermarkPredictors)
    {
        predictor->initialize(ingestionTimesForWatermarks.at(origin));
    }

    const auto now = std::chrono::system_clock::now();
    const auto duration = now.time_since_epoch();
    const auto timeNow = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
    auto minWatermark = Timestamp(0);
    for (const auto& [_, predictor] : watermarkPredictors)
    {
        minWatermark = std::min(minWatermark, predictor->getEstimatedWatermark(timeNow));
    }
    const auto slicesToReadBack = getTriggerableSlices(minWatermark);
    // TODO write out every slice that is not in slicesToReadBack

    /// Sort slices by state size to offload larger slices first and choose which ones can be efficiently written to external storage
    std::multimap<size_t, std::pair<std::shared_ptr<Slice>, FileLayout>> slicesToBeOffloadedBySize;
    //for (const auto& [sliceEnd, slice] : *windows.rlock())
    for (const auto& [sliceEnd, slice] : *slices.rlock())
    {
        //const auto nextWindowTrigger = Predictor.predict(windowEnd, newGlobalWatermark);

        // TODO exclude all slices that have already been read back from ssd or are going to be read back below
        const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
        if (const auto stateSize = nljSlice->getStateSizeInBytesForThreadId(memoryLayout, joinBuildSide, threadId);
            stateSize > USE_MIN_STATE_SIZE_WRITE)
        {
            memCtrl.setFileLayout(sliceEnd, threadId, joinBuildSide, USE_FILE_LAYOUT);
            slicesToBeOffloadedBySize.emplace(stateSize, std::make_pair(slice, USE_FILE_LAYOUT));
        }
    }

    /// Write all selected slices to disk, beginning with the largest ones
    const auto slicesInMemoryLocked = slicesInMemory.wlock();
    for (auto it = slicesToBeOffloadedBySize.rbegin(); it != slicesToBeOffloadedBySize.rend(); ++it)
    {
        const auto& [slice, fileLayout] = it->second;
        const auto sliceEnd = slice->getSliceEnd();

        /// Prevent other threads from combining pagedVectors to preserve data integrity as pagedVectors are not thread-safe
        const auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
        nljSlice->acquireCombinePagedVectorsMutex();

        /// If the pagedVectors have been combined then the slice was already emitted to probe and is being joined momentarily
        if (!nljSlice->pagedVectorsCombined())
        {
            (*slicesInMemoryLocked)[{sliceEnd, joinBuildSide}] = false;
            auto fileWriter = memCtrl.getFileWriter(sliceEnd, threadId, joinBuildSide);
            auto* const pagedVector = nljSlice->getPagedVectorRef(joinBuildSide, threadId);
            pagedVector->writeToFile(bufferProvider, memoryLayout, *fileWriter, fileLayout);
            pagedVector->truncate(fileLayout);
            // TODO force flush FileWriter?
        }

        nljSlice->releaseCombinePagedVectorsMutex();
    }

    /*
    /// Predict which slices to read back from external storage device
    std::vector<std::shared_ptr<Slice>> slicesToBeReadBack;
    const auto slicesLocked = slices.rlock();
    for (const auto& sliceEnd : sliceAssigner.getAllSliceEndTs(newGlobalWatermark))
    {
        const auto& [_, slice] = *slicesLocked->find(sliceEnd);
        // TODO predictiveRead()
    }

    /// Read all predicted slices back to main memory
    for (const auto& slice : slicesToBeReadBack)
    {
        // TODO slices are not locked and might still be filled!!!
        readSliceFromFiles(slice, bufferProvider, memoryLayout, QueryCompilation::JoinBuildSideType::Left, numberOfWorkerThreads);
        readSliceFromFiles(slice, bufferProvider, memoryLayout, QueryCompilation::JoinBuildSideType::Right, numberOfWorkerThreads);
    }
    */
}

void FileBackedTimeBasedSliceStore::readSliceFromFiles(
    const std::shared_ptr<Slice>& slice,
    Memory::AbstractBufferProvider* bufferProvider,
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t numberOfWorkerThreads)
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
        if (auto fileReader = memCtrl.getFileReader(sliceEnd, WorkerThreadId(threadId), joinBuildSide))
        {
            const auto fileLayout = memCtrl.getFileLayout(sliceEnd, WorkerThreadId(threadId), joinBuildSide);
            const auto pagedVector = std::dynamic_pointer_cast<NLJSlice>(slice)->getPagedVectorRef(joinBuildSide, WorkerThreadId(threadId));
            pagedVector->readFromFile(bufferProvider, memoryLayout, *fileReader, fileLayout.value());
            memCtrl.deleteFileLayout(sliceEnd, WorkerThreadId(threadId), joinBuildSide);
        }
    }
    (*slicesInMemoryLocked)[{sliceEnd, joinBuildSide}] = true;
}

std::map<SliceEnd, std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getTriggerableSlices(const Timestamp globalWatermark)
{
    std::map<SliceEnd, std::shared_ptr<Slice>> slices;
    for (auto& [windowInfo, windowSlicesAndState] : *windows.rlock())
    {
        if (windowInfo.windowEnd >= globalWatermark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        if (windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            /// This window has already been triggered
            continue;
        }

        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            slices.try_emplace(slice->getSliceEnd(), slice);
        }
    }
    return slices;
}

void FileBackedTimeBasedSliceStore::measureReadAndWriteExecTimes(const std::array<size_t, USE_TEST_DATA_SIZES.size()>& dataSizes)
{
    for (const auto dataSize : dataSizes)
    {
        std::vector<char> data(dataSize);
        for (size_t i = 0; i < dataSize; ++i)
        {
            //data[i] = static_cast<char>(rand() % 256);
            data[i] = static_cast<char>(i);
        }

        Timer timer("ReadAndWriteTimer");
        timer.start();

        const auto fileWriter = memCtrl.getFileWriter(
            SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(WorkerThreadId::INVALID), QueryCompilation::JoinBuildSideType::Left);
        fileWriter->write(data.data(), dataSize);
        timer.snapshot("write execution");

        const auto fileReader = memCtrl.getFileReader(
            SliceEnd(SliceEnd::INVALID_VALUE), WorkerThreadId(WorkerThreadId::INVALID), QueryCompilation::JoinBuildSideType::Left);
        fileReader->read(data.data(), dataSize);
        timer.snapshot("read execution");

        auto snapshots = timer.getSnapshots();
        writeExecTimes.emplace(dataSize, snapshots[0].getPrintTime());
        readExecTimes.emplace(dataSize, snapshots[1].getPrintTime());
        std::cout << "ExecTimes for " << dataSize << " Bytes: WriteExec: " << snapshots[0].getPrintTime()
                  << "ms ReadExec: " << snapshots[1].getPrintTime() << "ms\n";
    }
}

std::pair<double, double> FileBackedTimeBasedSliceStore::getReadAndWriteExecTimesForDataSize(const size_t dataSize)
{
    size_t closestKey = std::numeric_limits<size_t>::max();
    size_t minDifference = std::numeric_limits<size_t>::max();
    bool foundClosest = false;

    for (const auto& size : USE_TEST_DATA_SIZES)
    {
        const size_t difference = std::abs(static_cast<long long>(size) - static_cast<long long>(dataSize));
        if (difference < minDifference)
        {
            minDifference = difference;
            closestKey = size;
        }
        else if (foundClosest)
        {
            // If the difference starts increasing, we have found the closest key as test data sizes are sorted
            break;
        }
        foundClosest = true;
    }

    return {readExecTimes[closestKey], writeExecTimes[closestKey]};
}

}
