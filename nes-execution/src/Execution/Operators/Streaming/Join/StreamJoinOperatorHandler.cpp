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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/StateSerializationUtil.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <fstream>
#include <map>

namespace NES::Runtime::Execution::Operators {
void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr pipelineCtx, uint32_t) {
    NES_INFO("Started StreamJoinOperatorHandler!");
    setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    setBufferManager(pipelineCtx->getBufferManager());
}

void StreamJoinOperatorHandler::stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr pipelineCtx) {
    NES_INFO("Stopped StreamJoinOperatorHandler with {}!", magic_enum::enum_name(queryTerminationType));
    if (setForReuse) {
        return;
    }
    if (queryTerminationType == QueryTerminationType::Graceful) {
        triggerAllSlices(pipelineCtx.get());
    }
}

void StreamJoinOperatorHandler::recreate() {
    if (shouldBeRecreated) {
        recreateOperatorHandlerFromFile();
    }
};

bool StreamJoinOperatorHandler::shouldRecreate() {
    return shouldBeRecreated;
};

void StreamJoinOperatorHandler::recreateOperatorHandlerFromFile() {
    NES_INFO("Start of recreation from file");
    if (!recreationFilePath.has_value()) {
        NES_ERROR("Error: file path for recreation is not set");
    }

    auto filePath = recreationFilePath.value();
    // NES_ERROR("waiting for recreation file");
    while (!std::filesystem::exists(filePath)) {
        // NES_DEBUG("File {} does not exist yet", filePath);
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    // NES_ERROR("started recreating at {}", time);
    // NES_DEBUG("File {} exists. Start of recreation from file", filePath);
    std::ifstream fileStream(filePath, std::ios::binary | std::ios::in);

    if (!fileStream.is_open()) {
        NES_ERROR("Error: Failed to open file with the state for reading. Path is wrong.");
    }

    // 1. Read number of state buffers and number of window info buffers
    auto numberOfMetadataBuffers = 1;
    auto metadataBuffers = readBuffers(fileStream, numberOfMetadataBuffers);
    auto metadataPtr = metadataBuffers[0].getBuffer<uint64_t>();
    uint64_t numberOfWatermarkBuffers = metadataPtr[0];
    NES_DEBUG("Number of read watermark buffers: {}", numberOfWatermarkBuffers);
    uint64_t numberOfStateBuffers = metadataPtr[1];
    NES_DEBUG("Number of read state buffers: {}", numberOfStateBuffers);
    uint64_t numberOfWindowInfoBuffers = metadataPtr[2];
    NES_DEBUG("Number of read window info buffers: {}", numberOfWindowInfoBuffers);
    currentSliceId = metadataPtr[3];
    sequenceNumber = metadataPtr[4];

    // 2. read watermarks buffers
    auto recreatedWatermarkBuffers = readBuffers(fileStream, numberOfWatermarkBuffers);

    // 3. read state buffers
    auto recreatedStateBuffers = readBuffers(fileStream, numberOfStateBuffers);

    // 4. read window info buffers
    auto recreatedWindowInfoBuffers = readBuffers(fileStream, numberOfWindowInfoBuffers);

    fileStream.close();

    // 5. recreate state and window info from read buffers
    restoreWatermarks(recreatedWatermarkBuffers);
    restoreState(recreatedStateBuffers);
    restoreWindowInfo(recreatedWindowInfoBuffers);
    // reset recreation flag and delete file
    shouldBeRecreated = false;
    auto endTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    // NES_ERROR("State finished recreating at {}", endTime);
    if (std::remove(recreationFilePath->c_str()) == 0) {
        NES_DEBUG("File {} was removed successfully", recreationFilePath->c_str());
    } else {
        NES_WARNING("File {} was not deleted after recreation", recreationFilePath->c_str());
    }
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::serializeOperatorHandlerForMigration() {
    auto startTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    // NES_ERROR("Started serializing at {}", startTime);
    // get timestamp of not probed slices
    auto migrationTimestamp =
        std::min(watermarkProcessorBuild->getCurrentWatermark(), watermarkProcessorProbe->getCurrentWatermark());
    // NES_ERROR("watermark processors numbers {}, {}", watermarkProcessorBuild->getCurrentWatermark(), watermarkProcessorProbe->getCurrentWatermark());
    // NES_ERROR("serializing state from {} to {}", migrationTimestamp, UINT64_MAX);

    // add sizes to metadata
    auto metadata = bufferManager->getBufferBlocking();
    auto metadataPtr = metadata.getBuffer<uint64_t>();
    metadata.setSequenceNumber(++lastMigratedSeqNumber);

    // get watermarks, state and windows info to migrate
    auto watermarkBuffers = getWatermarksToMigrate();
    auto stateBuffers = getStateToMigrate(migrationTimestamp, UINT64_MAX);
    auto windowInfoBuffers = getWindowInfoToMigrate();

    metadataPtr[0] = watermarkBuffers.size();
    NES_DEBUG("Number of serialized watermark buffers: {}", watermarkBuffers.size());
    metadataPtr[1] = stateBuffers.size();
    NES_DEBUG("Number of serialized state buffers: {}", stateBuffers.size());
    metadataPtr[2] = windowInfoBuffers.size();
    NES_DEBUG("Number of serialized window info buffers: {}", windowInfoBuffers.size());
    metadataPtr[3] = currentSliceId;
    NES_DEBUG("Current probe sequence number: {}", sequenceNumber);
    metadataPtr[4] = sequenceNumber;

    std::vector<TupleBuffer> mergedBuffers = {metadata};

    mergedBuffers.insert(mergedBuffers.end(),
                         std::make_move_iterator(watermarkBuffers.begin()),
                         std::make_move_iterator(watermarkBuffers.end()));

    mergedBuffers.insert(mergedBuffers.end(),
                         std::make_move_iterator(stateBuffers.begin()),
                         std::make_move_iterator(stateBuffers.end()));

    mergedBuffers.insert(mergedBuffers.end(),
                         std::make_move_iterator(windowInfoBuffers.begin()),
                         std::make_move_iterator(windowInfoBuffers.end()));
    NES_DEBUG("Total number of serialized buffers: {}", mergedBuffers.size());
    for (auto& buffer: mergedBuffers) {
        buffer.setWatermark(mergedBuffers.size());
    }
    auto endTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    stateToTransfer = mergedBuffers;
    // NES_ERROR("Finished serializing at {}", endTime);
    return mergedBuffers;
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::getSerializedPortion(uint64_t id) {
    if (asked[id]) {
        return {};
    }
    asked[id] = true;
    auto numberOfThreads = 1;
    size_t totalSize = stateToTransfer.size();
    size_t chunkSize = (totalSize + numberOfThreads - 1) / numberOfThreads;

    size_t startIdx = id * chunkSize;
    size_t endIdx = std::min(startIdx + chunkSize, totalSize);

    std::vector<Runtime::TupleBuffer> portion;
    for (size_t i = id; i < totalSize; i += numberOfThreads) {
        portion.push_back(stateToTransfer[i]);
    }

    return portion;
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::getWatermarksToMigrate() {
    auto watermarksBuffers = std::vector<Runtime::TupleBuffer>();

    // metadata buffer
    auto buffer = bufferManager->getBufferBlocking();
    watermarksBuffers.emplace_back(buffer);
    uint64_t buffersCount = 1;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!buffer.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    // metadata pointer
    auto bufferPtr = buffer.getBuffer<uint64_t>();
    uint64_t bufferIdx = 0;

    /** @brief Lambda to write to watermark buffers */
    auto writeToBuffer = [&buffer, &bufferPtr, &bufferIdx, this, &buffersCount, &watermarksBuffers](uint64_t dataToWrite) {
        StateSerializationUtil::writeToBuffer(bufferManager,
                                              buffer.getBufferSize(),
                                              bufferPtr,
                                              bufferIdx,
                                              buffersCount,
                                              watermarksBuffers,
                                              dataToWrite);
    };

    auto buildWatermarksBuffers = watermarkProcessorBuild->serializeWatermarks(bufferManager);
    NES_DEBUG("build buffers size {}", buildWatermarksBuffers.size());

    // NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    // 1. Insert number of build origins to metadata buffer
    writeToBuffer(buildWatermarksBuffers.size());
    auto probeWatermarks = watermarkProcessorProbe->serializeWatermarks(bufferManager);
    NES_DEBUG("probe buffers size {}", probeWatermarks.size());

    // 3. Insert number of probe origins to metadata buffer
    writeToBuffer(probeWatermarks.size());

    // 4. insert build and probe buffers
    watermarksBuffers.insert(watermarksBuffers.end(), buildWatermarksBuffers.begin(), buildWatermarksBuffers.end());
    watermarksBuffers.insert(watermarksBuffers.end(), probeWatermarks.begin(), probeWatermarks.end());

    // set order of tuple buffers
    // TODO: #5027 change if getStateToMigrate will be used by several threads
    for (auto& buffer : watermarksBuffers) {
        buffer.setSequenceNumber(++lastMigratedSeqNumber);
    }

    return watermarksBuffers;
}

void StreamJoinOperatorHandler::restoreWatermarks(std::vector<Runtime::TupleBuffer>& buffers) {
    // get data buffer
    uint64_t dataBuffersIdx = 0;
    auto dataPtr = buffers[dataBuffersIdx].getBuffer<uint64_t>();
    uint64_t dataIdx = 0;

    /** @brief Lambda to read from data buffers
     * captured variables:
     * dataPtr - pointer to the start of current metadata buffer
     * dataIdx - index of size64_t data inside metadata buffer
     * dataBuffersIdx - index of current buffer to read
     * buffers - vector of buffers
    */
    auto deserializeNextValue = [&dataPtr, &dataIdx, &dataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(dataPtr, dataIdx, dataBuffersIdx, buffers);
    };

    // NOTE: Do not change the order of reads from data buffers(order is documented in function declaration)
    // 1. Retrieve number of build origins
    auto numberOfBuildBuffers = deserializeNextValue();
    watermarkProcessorBuild->restoreWatermarks(
        std::span<const Runtime::TupleBuffer>(buffers.data() + dataBuffersIdx + 1, numberOfBuildBuffers));

    // 3. Retrieve number of build origins
     auto numberOfProbeBuffers = deserializeNextValue();
    watermarkProcessorProbe->restoreWatermarks(
        std::span<const Runtime::TupleBuffer>(buffers.data() + numberOfBuildBuffers + dataBuffersIdx + 1, numberOfProbeBuffers));
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::getStateToMigrate(uint64_t startTS, uint64_t stopTS) {
    auto slicesLocked = slices.rlock();

    std::list<StreamSlicePtr> filteredSlices;
    // filtering slices, which start is in [startTS, stopTS) or end is in (startTS, stopTS]
    // (records are in range [start, end) in slice)
    std::copy_if(slicesLocked->begin(),
                 slicesLocked->end(),
                 std::back_inserter(filteredSlices),
                 [&startTS, &stopTS](const StreamSlicePtr& slice) {
                     uint64_t sliceStartTS = slice->getSliceStart();
                     uint64_t sliceEndTS = slice->getSliceEnd();
                     return (sliceStartTS >= startTS && sliceStartTS < stopTS) || (sliceEndTS > startTS && sliceEndTS < stopTS);
                 });

    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    // metadata buffer
    auto mainMetadata = bufferManager->getBufferBlocking();
    uint64_t metadataBuffersCount = 0;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!mainMetadata.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    // metadata pointer
    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    uint64_t metadataIdx = 1;

    /** @brief Lambda to write to metadata buffers */
    auto writeToMetadata =
        [&mainMetadata, &metadataPtr, &metadataIdx, this, &metadataBuffersCount, &buffersToTransfer](uint64_t dataToWrite) {
            StateSerializationUtil::writeToBuffer(bufferManager,
                                                  mainMetadata.getBufferSize(),
                                                  metadataPtr,
                                                  metadataIdx,
                                                  metadataBuffersCount,
                                                  buffersToTransfer,
                                                  dataToWrite);
        };

    // NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    // 1. Insert number of slices to metadata buffer
    writeToMetadata(filteredSlices.size());

//    auto countLeft = 0;
//    auto countRight = 0;
    for (const auto& slice : filteredSlices) {
        // NES_ERROR("slice start {}, slice end {}, slice id {}, numberOfTuplesLeft {}, numberOfTuplesRight {}", slice.get()->getSliceStart(), slice.get()->getSliceEnd(), slice.get()->getSliceId(), slice.get()->getNumberOfTuplesLeft(), slice.get()->getNumberOfTuplesRight());
        // get buffers with records and store
        auto sliceBuffers = slice->serialize(bufferManager);
        buffersToTransfer.insert(buffersToTransfer.end(), sliceBuffers.begin(), sliceBuffers.end());

        // 2. Insert number of buffers in i-th slice to metadata buffer
        writeToMetadata(sliceBuffers.size());
//        countLeft += slice.get()->getNumberOfTuplesLeft();
//        countRight += slice.get()->getNumberOfTuplesRight();
//        NES_ERROR("SERIALIZE SLICE");
//        slice->toString();
    }

//    for (auto slice: *slicesLocked) {
//        NES_ERROR("OH slice start {}, slice end {}, id {}, numLeft {}, numRight {}", slice.get()->getSliceStart(), slice.get()->getSliceEnd(), slice.get()->getSliceId(),slice.get()->getNumberOfTuplesLeft(), slice.get()->getNumberOfTuplesRight());
//    }

//    NES_ERROR("serialize tuples left {}, right {}", countLeft, countRight);

    // 3. set number of metadata buffers with the main to the first metadata buffer
    mainMetadata.getBuffer<uint64_t>()[0] = ++metadataBuffersCount;
    // insert first metadata buffer
    buffersToTransfer.emplace(buffersToTransfer.begin(), mainMetadata);

    // set order of tuple buffers
    // TODO: #5027 change if getStateToMigrate will be used by several threads
    for (auto& buffer : buffersToTransfer) {
        buffer.setSequenceNumber(++lastMigratedSeqNumber);
    }

    return buffersToTransfer;
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::getWindowInfoToMigrate() {
    auto windowToSliceLocked = windowToSlices.rlock();
    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    // metadata buffer
    auto dataBuffer = bufferManager->getBufferBlocking();
    uint64_t dataBuffersCount = 0;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!dataBuffer.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    // get buffer data pointer
    auto dataInBufferPtr = dataBuffer.getBuffer<uint64_t>();
    // starting from 1st index to later write number of windows to the 0 place in the buffer
    uint64_t dataInBufferIdx = 1;

    /** @brief Lambda to write to data buffers */
    auto serializeNextValue =
        [&dataBuffer, &dataInBufferPtr, &dataInBufferIdx, this, &dataBuffersCount, &buffersToTransfer](uint64_t dataToWrite) {
            StateSerializationUtil::writeToBuffer(bufferManager,
                                                  dataBuffer.getBufferSize(),
                                                  dataInBufferPtr,
                                                  dataInBufferIdx,
                                                  dataBuffersCount,
                                                  buffersToTransfer,
                                                  dataToWrite);
        };

    // counter for number of windows
    auto numOfWindowsToMigrate = 0;

    // go over windows and write its data to buffer
    for (const auto& window : *windowToSliceLocked) {
        // not serializing windows that are emitted to probe
        switch (window.second.windowState) {
            case WindowInfoState::EMITTED_TO_PROBE: continue;
            case WindowInfoState::BOTH_SIDES_FILLING:
            case WindowInfoState::ONCE_SEEN_DURING_TERMINATION:
                auto windowInfo = window.first;
                // 1. Write window start to buffer
                serializeNextValue(windowInfo.windowStart);
                // 2. Write window end to buffer
                serializeNextValue(windowInfo.windowEnd);

                auto slicesInfo = window.second;
                // 3. Write state info to buffer
                serializeNextValue(magic_enum::enum_integer(slicesInfo.windowState));
                // update windows counter
                numOfWindowsToMigrate++;
        }
    }

    // 4. Write number of windows
    dataBuffer.getBuffer<uint64_t>()[0] = numOfWindowsToMigrate;
    // insert first data buffer
    buffersToTransfer.emplace(buffersToTransfer.begin(), dataBuffer);

    // 5. set sequence numbers, starting from 1
    // TODO: #5027 change if getStateToMigrate will be used by several threads
    for (auto& buffer : buffersToTransfer) {
        buffer.setSequenceNumber(++lastMigratedSeqNumber);
    }

    return buffersToTransfer;
}

void StreamJoinOperatorHandler::restoreWindowInfo(std::vector<Runtime::TupleBuffer>& buffers) {
    // get data buffer
    uint64_t dataBuffersIdx = 0;
    auto dataPtr = buffers[dataBuffersIdx].getBuffer<uint64_t>();
    uint64_t dataIdx = 0;

    /** @brief Lambda to read from data buffers */
    auto deserializeNextValue = [&dataPtr, &dataIdx, &dataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(dataPtr, dataIdx, dataBuffersIdx, buffers);
    };

    // NOTE: Do not change the order of reads from data buffers(order is documented in function declaration)
    // 1. Retrieve number of windows
    auto numberOfWindows = deserializeNextValue();
    for (auto i = 0ULL; i < numberOfWindows; i++) {
        // 2. Retrieve window start
        auto windowStart = deserializeNextValue();
        // 3. Retrieve window end
        auto windowEnd = deserializeNextValue();
        // 4. Retrieve window status
        auto windowInfoState = magic_enum::enum_cast<WindowInfoState>(deserializeNextValue()).value();

        auto slicesLocked = this->slices.rlock();

        // slices that belongs to current window
        std::vector<StreamSlicePtr> slicesInWindow;
        std::copy_if(slicesLocked->begin(),
                     slicesLocked->end(),
                     std::back_inserter(slicesInWindow),
                     [&windowStart, &windowEnd](const StreamSlicePtr& slice) {
                         uint64_t sliceStartTS = slice->getSliceStart();
                         uint64_t sliceEndTS = slice->getSliceEnd();
                         return (sliceStartTS >= windowStart && sliceStartTS < windowEnd)
                             || (sliceEndTS > windowStart && sliceEndTS < windowEnd);
                     });

        auto windowToSlicesLocked = this->windowToSlices.wlock();
        windowToSlicesLocked->emplace(WindowInfo(windowStart, windowEnd), SlicesAndState{slicesInWindow, windowInfoState});
    }
}

void StreamJoinOperatorHandler::restoreState(std::vector<Runtime::TupleBuffer>& buffers) {

    // get main metadata buffer
    uint64_t metadataBuffersIdx = 0;
    auto metadataPtr = buffers[metadataBuffersIdx].getBuffer<uint64_t>();
    uint64_t metadataIdx = 0;

    // read number of metadata buffers
    auto numberOfMetadataBuffers = metadataPtr[metadataIdx++];

    /** @brief Lambda to read from metadata buffers */
    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(metadataPtr, metadataIdx, metadataBuffersIdx, buffers);
    };
    // NOTE: Do not change the order of reads from metadata (order is documented in function declaration)
    // 1. Retrieve number of slices from metadata buffer
    auto numberOfSlices = readFromMetadata();

    auto buffIdx = 0UL;
    auto slicesLocked = this->slices.wlock();

    // recreate slices from buffers
//    auto countLeft = 0;
//    auto countRight = 0;
    for (auto sliceIdx = 0UL; sliceIdx < numberOfSlices; ++sliceIdx) {

        // 2. Retrieve number of buffers in i-th slice
        auto numberOfBuffers = readFromMetadata();

        const auto spanStart = buffers.data() + numberOfMetadataBuffers + buffIdx;
        auto recreatedSlice = deserializeSlice(std::span<Runtime::TupleBuffer>(spanStart, numberOfBuffers));
        // NES_ERROR("slice start {}, slice end {}, slice id {}, numberOfTuplesLeft {}, numberOfTuplesRight {}", recreatedSlice.get()->getSliceStart(), recreatedSlice.get()->getSliceEnd(), recreatedSlice.get()->getSliceId(), recreatedSlice.get()->getNumberOfTuplesLeft(), recreatedSlice.get()->getNumberOfTuplesRight());
//        NES_ERROR("DESERIALIZE SLICE");
//        recreatedSlice->toString();
//        countLeft += recreatedSlice.get()->getNumberOfTuplesLeft();
//        countRight += recreatedSlice.get()->getNumberOfTuplesRight();
        // insert recreated slice
        auto indexToInsert = std::find_if(slicesLocked->begin(),
                                          slicesLocked->end(),
                                          [&recreatedSlice](const std::shared_ptr<StreamSlice>& currSlice) {
                                              return recreatedSlice->getSliceStart() > currSlice->getSliceEnd();
                                          });
        slicesLocked->emplace(indexToInsert, recreatedSlice);
        buffIdx += numberOfBuffers;
    }
    // NES_ERROR("recreate tuples left {}, right {}", countLeft, countRight);
    // NES_ERROR("num of slices {}", slicesLocked->size());
//    for (auto slice: *slicesLocked) {
//        NES_ERROR("OH slice start {}, slice end {}, id {}, numLeft {}, numRight {}", slice.get()->getSliceStart(), slice.get()->getSliceEnd(), slice.get()->getSliceId(),slice.get()->getNumberOfTuplesLeft(), slice.get()->getNumberOfTuplesRight());
//    }
}

void StreamJoinOperatorHandler::restoreStateFromFile(std::ifstream& stream) {
    std::vector<TupleBuffer> recreatedBuffers = {};

    while (!stream.eof()) {
        // read next buffer from file
        auto newBuffers = readBuffers(stream, 1);
        recreatedBuffers.insert(recreatedBuffers.end(), newBuffers.begin(), newBuffers.end());
    }

    // restore state from tuple buffers
    restoreState(recreatedBuffers);
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(uint64_t sliceIdentifier) {
    {
        auto slicesLocked = slices.rlock();
        for (auto& curSlice : *slicesLocked) {
            if (curSlice->getSliceIdentifier() == sliceIdentifier) {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}

std::optional<StreamSlicePtr>
StreamJoinOperatorHandler::getSliceByStartEnd(const WLockedSlices& slicesLocked, uint64_t sliceStart, uint64_t sliceEnd) {
    {
        for (auto& curSlice : *slicesLocked) {
            if (curSlice->getSliceStart() == sliceStart && curSlice->getSliceEnd() == sliceEnd && curSlice->isMutable()) {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}

void StreamJoinOperatorHandler::triggerAllSlices(PipelineExecutionContext* pipelineCtx) {
    {
        NES_INFO("triggering all slices")
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
            switch (slicesAndStateForWindow.windowState) {
                case WindowInfoState::BOTH_SIDES_FILLING:
                    slicesAndStateForWindow.windowState = WindowInfoState::ONCE_SEEN_DURING_TERMINATION;
                case WindowInfoState::EMITTED_TO_PROBE: continue;
                case WindowInfoState::ONCE_SEEN_DURING_TERMINATION: {
                    slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;

                    // Performing a cross product of all slices to make sure that each slice gets probe with each other slice
                    // For bucketing, this should be only done once
                    for (auto& sliceLeft : slicesAndStateForWindow.slices) {
                        for (auto& sliceRight : slicesAndStateForWindow.slices) {
                            emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                        }
                    }
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteAllSlices() {
    {
        if (setForReuse || migrating) {
            return;
        }
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        slicesLocked->clear();
        windowToSlicesLocked->clear();
    }
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData,
                                                       PipelineExecutionContext* pipelineCtx) {
    // The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark =
        watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());
    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
            if (windowInfo.windowEnd > newGlobalWatermark
                || slicesAndStateForWindow.windowState == WindowInfoState::EMITTED_TO_PROBE) {
                // This window can not be triggered yet or has already been triggered
                continue;
            }
            slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;
            NES_INFO("Emitting all slices for window {}", windowInfo.toString());

            // Performing a cross product of all slices to make sure that each slice gets probe with each other slice
            // For bucketing, this should be only done once
            for (auto& sliceLeft : slicesAndStateForWindow.slices) {
                for (auto& sliceRight : slicesAndStateForWindow.slices) {
                    // NES_ERROR("emit slices LEFT: start {}, end {}, id {}, numLeft {}, numRight {}, RIGHT: start {}, end {}, id {}, numLeft {}, numRight {}", sliceLeft.get()->getSliceStart(), sliceLeft.get()->getSliceEnd(), sliceLeft.get()->getSliceId(), sliceLeft.get()->getNumberOfTuplesLeft(), sliceLeft.get()->getNumberOfTuplesRight(),
//                              sliceRight.get()->getSliceStart(), sliceRight.get()->getSliceEnd(), sliceRight.get()->getSliceId(), sliceRight.get()->getNumberOfTuplesLeft(), sliceRight.get()->getNumberOfTuplesRight());
                    // NES_ERROR("SLICE");
                    // sliceLeft->toString();
                    emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteSlices(const BufferMetaData& bufferMetaData) {
    uint64_t newGlobalWaterMarkProbe =
        watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    // NES_ERROR("newGlobalWaterMarkProbe {} bufferMetaData {} seqNumber {}", newGlobalWaterMarkProbe, bufferMetaData.toString(), bufferMetaData.seqNumber.toString());

    if (setForReuse) {
        return;
    }

    auto slicesLocked = slices.wlock();
    for (auto it = slicesLocked->begin(); it != slicesLocked->end(); ++it) {
        auto& curSlice = *it;

        if ((approach != SharedJoinApproach::APPROACH_PROBING
             && curSlice->getSliceStart() + windowSize
                 < newGlobalWaterMarkProbe)//In approach one #5114 a window can contain a slice that is not completely contained so we are only allowed to remove slices once there is no overlap
            || (curSlice->getSliceEnd() + windowSize < newGlobalWaterMarkProbe)) {
            // We can delete this slice/window
            NES_DEBUG("Deleting slice: {} as sliceStart+windowSize {} is smaller then watermark {}",
                      curSlice->toString(),
                      curSlice->getSliceStart() + windowSize,
                      newGlobalWaterMarkProbe);
            it = slicesLocked->erase(it);
        }
    }
}

uint64_t StreamJoinOperatorHandler::getNumberOfSlices() { return slices.rlock()->size(); }

uint64_t StreamJoinOperatorHandler::getNumberOfTuplesInSlice(uint64_t sliceIdentifier,
                                                             QueryCompilation::JoinBuildSideType buildSide) {
    auto slice = getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value()) {
        auto& sliceVal = slice.value();
        switch (buildSide) {
            case QueryCompilation::JoinBuildSideType::Left: return sliceVal->getNumberOfTuplesLeft();
            case QueryCompilation::JoinBuildSideType::Right: return sliceVal->getNumberOfTuplesRight();
        }
    }
    return -1;
}

OriginId StreamJoinOperatorHandler::getOutputOriginId() const { return outputOriginId; }

// TODO: migrate this sequenceNumber also??
uint64_t StreamJoinOperatorHandler::getNextSequenceNumber() { return sequenceNumber++; }

void StreamJoinOperatorHandler::setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) {
    if (StreamJoinOperatorHandler::alreadySetup) {
        NES_DEBUG("StreamJoinOperatorHandler::setup was called already!");
        return;
    }
    StreamJoinOperatorHandler::alreadySetup = true;

    NES_DEBUG("StreamJoinOperatorHandler::setup was called!");
    StreamJoinOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId) {
    workerThreadIdToWatermarkMap[workerThreadId] = watermark;
}

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() {
    auto minVal = std::min_element(std::begin(workerThreadIdToWatermarkMap),
                                   std::end(workerThreadIdToWatermarkMap),
                                   [](const auto& l, const auto& r) {
                                       return  l.second < r.second;
                                   });
    return minVal == workerThreadIdToWatermarkMap.end() ? -1 : minVal->second;
}

uint64_t StreamJoinOperatorHandler::getWindowSlide() const { return sliceAssigner.getWindowSlide(); }

uint64_t StreamJoinOperatorHandler::getWindowSize() const { return sliceAssigner.getWindowSize(); }

void StreamJoinOperatorHandler::setBufferManager(const NES::Runtime::BufferManagerPtr& bufManager) {
    this->bufferManager = bufManager;
}

std::map<QueryId, uint64_t> StreamJoinOperatorHandler::getQueriesAndDeploymentTimes() { return deploymentTimes; }

uint64_t StreamJoinOperatorHandler::getNextSliceId() {
    currentSliceId++;
    return currentSliceId;
}

void StreamJoinOperatorHandler::setApproach(SharedJoinApproach approach) { this->approach = approach; }

SharedJoinApproach StreamJoinOperatorHandler::getApproach() { return approach; }

void StreamJoinOperatorHandler::setRecreationFileName(std::string filePath) {
    shouldBeRecreated = true;
    recreationFilePath = filePath;
}

std::optional<std::string> StreamJoinOperatorHandler::getRecreationFileName() { return recreationFilePath; }

std::vector<TupleBuffer> StreamJoinOperatorHandler::readBuffers(std::ifstream& stream, uint64_t numberOfBuffers) {
    std::vector<TupleBuffer> readBuffers = {};

    for (auto currBuffer = 0ULL; currBuffer < numberOfBuffers; currBuffer++) {
        uint64_t size = 0, numberOfTuples = 0;
        // 1. read size of current buffer
        stream.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
        // 2. read number of tuples in current buffer
        stream.read(reinterpret_cast<char*>(&numberOfTuples), sizeof(uint64_t));

        // if size of the read buffer is more than size of pooled buffers in buffer manager
        if (size > bufferManager->getBufferSize()) {
            // then it is not implemented as it should be split into several smaller buffers
            NES_NOT_IMPLEMENTED();
        }
        if (size > 0) {
            //recreate vector of tuple buffers from the file
            auto newBuffer = bufferManager->getBufferBlocking();
            // 3. read buffer content
            stream.read(reinterpret_cast<char*>(newBuffer.getBuffer()), size);
            newBuffer.setNumberOfTuples(numberOfTuples);
            readBuffers.emplace_back(newBuffer);
        }
    }

    return readBuffers;
}

uint64_t getNLJSliceStartProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceStart();
}

uint64_t getNLJSliceEndProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceEnd();
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     const SchemaPtr& leftSchema,
                                                     const SchemaPtr& rightSchema,
                                                     TimeFunctionPtr leftTimeFunctionPtr,
                                                     TimeFunctionPtr rightTimeFunctionPtr,
                                                     std::map<QueryId, uint64_t> deploymentTimes)
    : numberOfWorkerThreads(1), windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)),
      watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector<OriginId>(1, outputOriginId))),
      outputOriginId(outputOriginId), sequenceNumber(1), sizeOfRecordLeft(leftSchema->getSchemaSizeInBytes()),
      sizeOfRecordRight(rightSchema->getSchemaSizeInBytes()), leftSchema(leftSchema), rightSchema(rightSchema),
      leftTimeFunctionPtr(std::move(leftTimeFunctionPtr)), rightTimeFunctionPtr(std::move(rightTimeFunctionPtr)),
      deploymentTimes(deploymentTimes),
      //sliceAssigner looks ugly. However, I would like to have a vector in slice assigner as it needs to make calculations for every tuple and it will be faster to iterate over a vector instead of a map. And I would like to keep deploymentTime connected to the queryId in the handler. It could be done with two vectors and updating them simultaneously, but a map is more explicit. Maybe some other data structure would work well here, or in sliceAssigner.
      sliceAssigner(windowSize, windowSlide, [&deploymentTimes]() {
          std::vector<uint64_t> deploymentTimesVec;
          for (const auto& p : deploymentTimes) {
              deploymentTimesVec.push_back(p.second);
          }
          return deploymentTimesVec;
      }()) {}

}// namespace NES::Runtime::Execution::Operators
