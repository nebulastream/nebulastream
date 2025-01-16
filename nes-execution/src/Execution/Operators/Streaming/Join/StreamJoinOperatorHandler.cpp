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
    if (shouldBeRecreated) {
        recreateOperatorHandlerFromFile();
    }
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

void StreamJoinOperatorHandler::recreateOperatorHandlerFromFile() {
    NES_INFO("Start of recreation from file");
    if (!recreationFilePath.has_value()) {
        NES_ERROR("Error: file path for recreation is not set");
    }

    auto filePath = recreationFilePath.value();

    while (!std::filesystem::exists(filePath)) {
        NES_DEBUG("File {} does not exist yet", filePath);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::ifstream fileStream(filePath, std::ios::binary | std::ios::in);

    if (!fileStream.is_open()) {
        NES_ERROR("Error: Failed to open file with the state for reading. Path is wrong.");
    }

    // 1. Read number of state buffers and number of window info buffers
    auto numberOfMetadataBuffers = 1;
    auto metadataBuffers = readBuffers(fileStream, numberOfMetadataBuffers);
    auto metadataPtr = metadataBuffers[0].getBuffer<uint64_t>();
    uint64_t numberOfStateBuffers = metadataPtr[0];
    uint64_t numberOfWindowInfoBuffers = metadataPtr[1];

    // 2. read state buffers
    std::vector<TupleBuffer> recreatedStateBuffers = readBuffers(fileStream, numberOfStateBuffers);

    // 3. read window info buffers
    std::vector<TupleBuffer> recreatedWindowInfoBuffers = readBuffers(fileStream, numberOfWindowInfoBuffers);

    fileStream.close();

    // 4. recreate state and window info from read buffers
    restoreState(recreatedStateBuffers);
    restoreWindowInfo(recreatedWindowInfoBuffers);
    // reset recreation flag and delete file
    shouldBeRecreated = false;
    std::remove(recreationFilePath->c_str());
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
    auto metadataBuffersCount = 0;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!mainMetadata.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    // metadata pointer
    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    auto metadataIdx = 1ULL;

    /** @brief Lambda to write to metadata buffers
     * captured variables:
     * mainMetadata - generic metadata buffer, used for checking space left
     * metadataPtr - pointer to the start of current metadata buffer
     * metadataIdx - index of size64_t data inside metadata buffer
     * metadataBuffersCount - number of metadata buffers
     * buffers - vector of buffers
     * @param dataToWrite - value to write to the buffer
    */
    auto writeToMetadata =
        [&mainMetadata, &metadataPtr, &metadataIdx, this, &metadataBuffersCount, &buffersToTransfer](uint64_t dataToWrite) {
            // check that current metadata buffer has enough space, by sending used space and space needed
            if (!mainMetadata.hasSpaceLeft(metadataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
                // if current buffer does not contain enough space then
                // get new buffer and insert to vector of buffers
                auto newBuffer = bufferManager->getBufferBlocking();
                buffersToTransfer.emplace(buffersToTransfer.begin() + metadataBuffersCount++, newBuffer);
                // update pointer and index
                metadataPtr = newBuffer.getBuffer<uint64_t>();
                metadataIdx = 0;
            }
            metadataPtr[metadataIdx++] = dataToWrite;
        };

    // NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    // 1. Insert number of slices to metadata buffer
    writeToMetadata(filteredSlices.size());

    for (const auto& slice : filteredSlices) {
        // get buffers with records and store
        auto sliceBuffers = slice->serialize(bufferManager);
        buffersToTransfer.insert(buffersToTransfer.end(), sliceBuffers.begin(), sliceBuffers.end());

        // 2. Insert number of buffers in i-th slice to metadata buffer
        writeToMetadata(sliceBuffers.size());
    }

    // 3. set number of metadata buffers with the main to the first metadata buffer
    mainMetadata.getBuffer<uint64_t>()[0] = ++metadataBuffersCount;
    // insert first metadata buffer
    buffersToTransfer.emplace(buffersToTransfer.begin(), mainMetadata);

    // set order of tuple buffers
    // TODO: #5027 change if getStateToMigrate will be used by several threads
    for (auto i = 0ULL; i < buffersToTransfer.size(); i++) {
        buffersToTransfer[i].setSequenceNumber(++lastMigratedSeqNumber);
    }

    return buffersToTransfer;
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::getWindowInfoToMigrate() {
    auto windowToSliceLocked = windowToSlices.rlock();
    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    // metadata buffer
    auto dataBuffer = bufferManager->getBufferBlocking();
    auto dataBuffersCount = 0;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!dataBuffer.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    // get buffer data pointer
    auto dataInBufferPtr = dataBuffer.getBuffer<uint64_t>();
    // starting from 1st index to later write number of windows to the 0 place in the buffer
    auto dataInBufferIdx = 1ULL;

    /** @brief Lambda to write to data buffers
     * captured variables:
     * dataBuffer - generic metadata buffer, used for checking space left
     * dataPtr - pointer to the start of current metadata buffer
     * metadataIdx - index of size64_t data inside metadata buffer
     * metadataBuffersCount - number of metadata buffers
     * buffersToTransfer - vector of buffers
     * @param dataToWrite - value to write to the buffer
    */
    auto serializeNextValue =
        [&dataBuffer, &dataInBufferPtr, &dataInBufferIdx, this, &dataBuffersCount, &buffersToTransfer](uint64_t dataToWrite) {
            // check that current metadata buffer has enough space, by sending used space and space needed
            if (!dataBuffer.hasSpaceLeft(dataInBufferIdx * sizeof(uint64_t), sizeof(uint64_t))) {
                // if current buffer does not contain enough space then
                // get new buffer and insert to vector of buffers
                auto newBuffer = bufferManager->getBufferBlocking();
                buffersToTransfer.emplace(buffersToTransfer.begin() + dataBuffersCount++, newBuffer);
                // update pointer and index
                dataInBufferPtr = newBuffer.getBuffer<uint64_t>();
                dataInBufferIdx = 0;
            }
            dataInBufferPtr[dataInBufferIdx++] = dataToWrite;
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
    for (auto i = 0ULL; i < buffersToTransfer.size(); i++) {
        buffersToTransfer[i].setSequenceNumber(++lastMigratedSeqNumber);
    }

    return buffersToTransfer;
}

void StreamJoinOperatorHandler::restoreWindowInfo(std::vector<Runtime::TupleBuffer>& buffers) {
    // get data buffer
    auto dataBuffersIdx = 0;
    auto dataPtr = buffers[dataBuffersIdx].getBuffer<uint64_t>();
    auto dataIdx = 0;

    /** @brief Lambda to read from data buffers
     * captured variables:
     * dataPtr - pointer to the start of current metadata buffer
     * dataIdx - index of size64_t data inside metadata buffer
     * dataBuffersIdx - index of current buffer to read
     * buffers - vector of buffers
    */
    auto deserializeNextValue = [&dataPtr, &dataIdx, &dataBuffersIdx, &buffers]() -> uint64_t {
        // check left space in metadata buffer
        if (!buffers[dataBuffersIdx].hasSpaceLeft(dataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
            // update metadata pointer and index
            dataPtr = buffers[++dataBuffersIdx].getBuffer<uint64_t>();
            dataIdx = 0;
        }
        return dataPtr[dataIdx++];
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
    auto metadataBuffersIdx = 0;
    auto metadataPtr = buffers[metadataBuffersIdx].getBuffer<uint64_t>();
    auto metadataIdx = 0;

    // read number of metadata buffers
    auto numberOfMetadataBuffers = metadataPtr[metadataIdx++];

    /** @brief Lambda to read from metadata buffers
     * captured variables:
     * metadataPtr - pointer to the start of current metadata buffer
     * metadataIdx - index of size64_t data inside metadata buffer
     * metadataBuffersIdx - index of current buffer to read
     * buffers - vector of buffers
    */
    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t {
        // check left space in metadata buffer
        if (!buffers[metadataBuffersIdx].hasSpaceLeft(metadataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
            // update metadata pointer and index
            metadataPtr = buffers[++metadataBuffersIdx].getBuffer<uint64_t>();
            metadataIdx = 0;
        }
        return metadataPtr[metadataIdx++];
    };
    // NOTE: Do not change the order of reads from metadata (order is documented in function declaration)
    // 1. Retrieve number of slices from metadata buffer
    auto numberOfSlices = readFromMetadata();

    auto buffIdx = 0UL;
    auto slicesLocked = this->slices.wlock();

    // recreate slices from buffers
    for (auto sliceIdx = 0UL; sliceIdx < numberOfSlices; ++sliceIdx) {

        // 2. Retrieve number of buffers in i-th slice
        auto numberOfBuffers = readFromMetadata();

        const auto spanStart = buffers.data() + numberOfMetadataBuffers + buffIdx;
        auto recreatedSlice = deserializeSlice(std::span<const Runtime::TupleBuffer>(spanStart, numberOfBuffers));

        // insert recreated slice
        auto indexToInsert = std::find_if(slicesLocked->begin(),
                                          slicesLocked->end(),
                                          [&recreatedSlice](const std::shared_ptr<StreamSlice>& currSlice) {
                                              return recreatedSlice->getSliceStart() > currSlice->getSliceEnd();
                                          });
        slicesLocked->emplace(indexToInsert, recreatedSlice);
        buffIdx += numberOfBuffers;
    }
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
        if (setForReuse) {
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
    // NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());
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
                    emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteSlices(const BufferMetaData& bufferMetaData) {
    uint64_t newGlobalWaterMarkProbe =
        watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWaterMarkProbe {} bufferMetaData {}", newGlobalWaterMarkProbe, bufferMetaData.toString());

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
                                       return l.second < r.second;
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
