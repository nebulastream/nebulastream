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

#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/StateSerializationUtil.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <tuple>

namespace NES::Runtime::Execution::Operators {

KeyedSlicePtr KeyedSlicePreAggregationHandler::deserializeSlice(std::span<const Runtime::TupleBuffer> buffers) {
        return KeyedSlice::deserialize(buffers);
    }

KeyedSlicePreAggregationHandler::KeyedSlicePreAggregationHandler(uint64_t windowSize,
                                                                 uint64_t windowSlide,
                                                                 const std::vector<OriginId>& origins)
    : AbstractSlicePreAggregationHandler<KeyedSlice, KeyedThreadLocalSliceStore>(windowSize, windowSlide, origins) {}

void KeyedSlicePreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                            uint64_t keySize,
                                            uint64_t valueSize) {
    NES_ASSERT(threadLocalSliceStores.empty(), "The thread local slice store must be empty");
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<KeyedThreadLocalSliceStore>(keySize, valueSize, windowSize, windowSlide);
        threadLocalSliceStores.emplace_back(std::move(threadLocal));
    }
    setBufferManager(ctx.getBufferManager());
}

std::vector<Runtime::TupleBuffer> KeyedSlicePreAggregationHandler::serializeOperatorHandlerForMigration() {
    // get timestamp of not probed slices
    auto migrationTimestamp =
        std::min(watermarkProcessor->getCurrentWatermark(), watermarkProcessor->getCurrentWatermark());

    // add sizes to metadata
    auto metadata = bufferManager->getBufferBlocking();
    auto metadataPtr = metadata.getBuffer<uint64_t>();
    metadata.setSequenceNumber(++resultSequenceNumber);

    // get watermarks, state and windows info to migrate
    auto watermarkBuffers = getWatermarksToMigrate();
    auto stateBuffers = getStateToMigrate(migrationTimestamp, UINT64_MAX);

    metadataPtr[0] = watermarkBuffers.size();
    NES_DEBUG("Number of serialized watermark buffers: {}", watermarkBuffers.size());
    metadataPtr[1] = stateBuffers.size();
    NES_DEBUG("Number of serialized state buffers: {}", stateBuffers.size());

    std::vector<TupleBuffer> mergedBuffers = {metadata};

    mergedBuffers.insert(mergedBuffers.end(),
                         std::make_move_iterator(watermarkBuffers.begin()),
                         std::make_move_iterator(watermarkBuffers.end()));

    mergedBuffers.insert(mergedBuffers.end(),
                         std::make_move_iterator(stateBuffers.begin()),
                         std::make_move_iterator(stateBuffers.end()));

    NES_DEBUG("Total number of serialized buffers: {}", mergedBuffers.size());
    return mergedBuffers;
}

std::vector<Runtime::TupleBuffer> KeyedSlicePreAggregationHandler::getStateToMigrate(uint64_t startTS, uint64_t stopTS) {
    // 1. Create a vector to hold all the buffers we will end up returning.
    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    // 2. Create (and reserve) a "metadata" buffer for high-level info:
    // how many slices in total, how many buffers per slice, etc.
    auto mainMetadata = bufferManager->getBufferBlocking();
    uint64_t metadataBuffersCount = 0;

    if (!mainMetadata.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }
    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    uint64_t metadataIdx = 1; // We leave index 0 for "metadata buffers count" (populated later)

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

    // We'll gather slices from *every* threadLocalSliceStore.
    // For each store, we get all slices that match [startTS, stopTS).
    // Then we serialize them and place the buffers in buffersToTransfer.
    uint64_t totalSlices = 0;
    std::vector<std::pair<uint64_t, std::vector<Runtime::TupleBuffer>>> serializedSlicesPerThread;
    serializedSlicesPerThread.reserve(threadLocalSliceStores.size());

    for (auto& sliceStorePtr : threadLocalSliceStores) {
        // 3. (Thread local) Acquire slices that overlap with [startTS, stopTS).
        // This part depends on how your SliceStore enumerates or filters slices.
        auto listSlice = std::move(sliceStorePtr->getSlices());
        std::list<KeyedSlicePtr> filteredSlices;

        // Filter slices that overlap with [startTS, stopTS)
        for (auto& slice : listSlice) {
            uint64_t sliceStartTS = slice->getStart();
            uint64_t sliceEndTS = slice->getEnd();

            if ((sliceStartTS >= startTS && sliceStartTS < stopTS) ||
                (sliceEndTS > startTS && sliceEndTS < stopTS)) {
                filteredSlices.emplace_back(std::move(slice));
                }
        }

        // 4. Serialize each slice. This is typically a method inside KeyedSlice:
        // e.g., slice->serialize(bufferManager) returning std::vector<TupleBuffer>.
        // Then count them.
        uint64_t threadSliceCount = 0;
        std::vector<Runtime::TupleBuffer> localSerialized;

        for (auto& slice : filteredSlices) {
            auto buffers = slice->serialize(bufferManager);
            threadSliceCount += 1;
            // Add all the returned buffers to local vector for now
            localSerialized.insert(localSerialized.end(), buffers.begin(), buffers.end());
        }
        //Keep track so we can write them to the final output after we handle metadata
        totalSlices += threadSliceCount;
        serializedSlicesPerThread.emplace_back(threadSliceCount, std::move(localSerialized));
    }
    // 5. Write the total number of slices across all thread locals to metadata
    writeToMetadata(totalSlices);

    // 6. For each threadLocal store’s slices:
    // - write how many slices are from that store
    // - for each slice, write how many buffers that slice used
    // - push the actual buffers
    for (auto& [sliceCount, serialized] : serializedSlicesPerThread) {
        for (auto& tupleBuf : serialized) {
            buffersToTransfer.push_back(std::move(tupleBuf));
            // buffersToTransfer.insert(buffersToTransfer.end(),
            //                      serialized.begin(),
            //                      serialized.end());
        }
    }

    // 7. Now set the count of metadata buffers in index 0 of the mainMetadata
    mainMetadata.getBuffer<uint64_t>()[0] = ++metadataBuffersCount;

    // Insert mainMetadata at the front
    buffersToTransfer.emplace(buffersToTransfer.begin(), mainMetadata);

    // 8. If you wish to set a sequence number on each buffer:
    for (auto i = 0ULL; i < buffersToTransfer.size(); i++) {
        buffersToTransfer[i].setSequenceNumber(resultSequenceNumber++);
    }
    return buffersToTransfer;
}

void KeyedSlicePreAggregationHandler::restoreState(std::vector<Runtime::TupleBuffer>& buffers) {
    // 1. read the main metadata buffer
    uint64_t metadataBuffersIdx = 0;
    auto metadataPtr = buffers[metadataBuffersIdx].getBuffer<uint64_t>();
    uint64_t metadataIdx = 0;

    // 2. read numberOfMetadataBuffers
    auto numberOfMetadataBuffers = metadataPtr[metadataIdx++];

    /** @brief Lambda to read from metadata buffers */
    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(metadataPtr, metadataIdx, metadataBuffersIdx, buffers);
    };

    // NOTE: Do not change the order of reads from metadata (order is documented in function declaration)
    // 3. read how many slices total
    auto numberOfSlices = readFromMetadata();

    for (auto& sliceStorePtr : threadLocalSliceStores) {
        // We'll keep track of the current offset in the buffers array
        // beyond the metadata:
        auto buffIdx = 0UL;
        auto listSlice = std::move(sliceStorePtr->getSlices());
        for (auto sliceIdx = 0UL; sliceIdx < numberOfSlices; ++sliceIdx) {
            // Retrieve number of buffers in i-th slice
            auto numberOfBuffers = readFromMetadata();

            const auto spanStart = buffers.data() + numberOfMetadataBuffers + buffIdx;
            auto recreatedSlice = deserializeSlice(std::span<const Runtime::TupleBuffer>(spanStart, numberOfBuffers));

            // insert recreated slice
            auto indexToInsert = std::find_if(listSlice.begin(),
                                              listSlice.end(),
                                              [&recreatedSlice](const std::unique_ptr<KeyedSlice>& currSlice) {
                                                  return recreatedSlice->getStart() > currSlice->getEnd();
                                              });
            listSlice.emplace(indexToInsert, std::move(recreatedSlice));
            buffIdx += numberOfBuffers;
        }
    }
}

std::vector<Runtime::TupleBuffer> KeyedSlicePreAggregationHandler::getWatermarksToMigrate() {
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

    auto serializedWatermarks = watermarkProcessor->serializeWatermarks(bufferManager);
    NES_DEBUG("build buffers size {}", serializedWatermarks.size());

    // NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    // 1. Insert number of build origins to metadata buffer
    writeToBuffer(serializedWatermarks.size());

    // 2. insert build and probe buffers
    watermarksBuffers.insert(watermarksBuffers.end(), serializedWatermarks.begin(), serializedWatermarks.end());

    // set order of tuple buffers
    for (auto& buffer : watermarksBuffers) {
        buffer.setSequenceNumber(++resultSequenceNumber);
    }

    return watermarksBuffers;
}

void KeyedSlicePreAggregationHandler::restoreWatermarks(std::vector<Runtime::TupleBuffer>& buffers) {
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
    // Retrieve number of build origins
    auto numberOfBuildBuffers = deserializeNextValue();
    watermarkProcessor->restoreWatermarks(
        std::span<const Runtime::TupleBuffer>(buffers.data() + dataBuffersIdx + 1, numberOfBuildBuffers));
}

void KeyedSlicePreAggregationHandler::setBufferManager(const NES::Runtime::BufferManagerPtr& bufManager) {
    this->bufferManager = bufManager;
}

KeyedSlicePreAggregationHandler::~KeyedSlicePreAggregationHandler() { NES_DEBUG("~GlobalSlicePreAggregationHandler"); }

}// namespace NES::Runtime::Execution::Operators
