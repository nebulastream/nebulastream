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

std::vector<TupleBuffer> KeyedSlicePreAggregationHandler::getStateToMigrate(uint64_t startTS, uint64_t stopTS) {
    std::vector<TupleBuffer> buffersToTransfer;

    // 1. create a "mainMetadata" buffer for high-level info
    auto mainMetadata = bufferManager->getBufferBlocking();
    uint64_t metadataBuffersCount = 0;
    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    uint64_t metadataIdx = 1;

    auto writeToMetadata =
            [this, &mainMetadata, &metadataPtr, &metadataIdx, &metadataBuffersCount, &buffersToTransfer](uint64_t data) {
                StateSerializationUtil::writeToBuffer(
                    bufferManager,
                    mainMetadata.getBufferSize(),
                    metadataPtr,
                    metadataIdx,
                    metadataBuffersCount,
                    buffersToTransfer,
                    data
                );
    };

    writeToMetadata(3ULL);
    writeToMetadata(lastTriggerWatermark.load(std::memory_order_relaxed));
    writeToMetadata(resultSequenceNumber.load(std::memory_order_relaxed));

    auto watermarkBuffers = getWatermarksToMigrate();
    writeToMetadata(watermarkBuffers.size());

    for (auto& wb : watermarkBuffers) {
        buffersToTransfer.push_back(std::move(wb));
    }

    std::vector<std::vector<TupleBuffer>> allSlices;
    allSlices.reserve(32);

    // 2. gather all slices from each thread store that overlap [startTS..stopTS)
    uint64_t totalSlices = 0;

    for (auto& sliceStorePtr : threadLocalSliceStores) {
        auto allStoreSlices = std::move(sliceStorePtr->getSlices());
        std::list<KeyedSlicePtr> filtered;

        for (auto& slice : allStoreSlices) {
            uint64_t sStart = slice->getStart();
            uint64_t sEnd   = slice->getEnd();
            bool overlaps = ((sStart >= startTS && sStart < stopTS) ||
                             (sEnd   >  startTS && sEnd   < stopTS));
            if (overlaps) {
                filtered.emplace_back(std::move(slice));
            }
        }

        for (auto& slice : filtered) {
            auto sliceBuffers = slice->serialize(bufferManager); // e.g. 2 buffers
            allSlices.push_back(std::move(sliceBuffers));
            totalSlices++;
        }
    }

    // 3. write the total number of slices
    writeToMetadata(totalSlices);

    // 4. for each slice, append numberOfBuffersForThisSlice
    for (auto& sliceBuffers : allSlices) {

        writeToMetadata(sliceBuffers.size());
        for (auto& buf : sliceBuffers) {
            buffersToTransfer.push_back(std::move(buf));
        }
    }

    // 5. store the updated mainMetadata buffer as the first element
    mainMetadata.getBuffer<uint64_t>()[0] = ++metadataBuffersCount;
    buffersToTransfer.insert(buffersToTransfer.begin(), mainMetadata);

    // 6. set sequence numbers
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

    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(
            metadataPtr,
            metadataIdx,
            metadataBuffersIdx,
            buffers
        );
    };
    auto aggregatorFieldCount = readFromMetadata();
    if (aggregatorFieldCount > 0) {
        auto lw = readFromMetadata();
        lastTriggerWatermark.store(lw, std::memory_order_relaxed);
    }
    if (aggregatorFieldCount > 1) {
        auto rsn = readFromMetadata();
        resultSequenceNumber.store(rsn, std::memory_order_relaxed);
    }

    auto numberOfWatermarkBuffers = readFromMetadata();
    std::vector<TupleBuffer> watermarkBuffers;
    watermarkBuffers.reserve(numberOfWatermarkBuffers);

    uint64_t bufferIndex = numberOfMetadataBuffers;

    for (auto i = 0ULL; i < numberOfWatermarkBuffers; i++) {
        watermarkBuffers.push_back(std::move(buffers[bufferIndex++]));
    }
    restoreWatermarks(watermarkBuffers);

    // 3. read how many slices total
    auto numberOfSlices = readFromMetadata();

    auto buffIdx = numberOfMetadataBuffers + numberOfWatermarkBuffers;

    for (auto& sliceStorePtr : threadLocalSliceStores) {
        auto listSlice = std::move(sliceStorePtr->getSlices());

        for (auto sliceIdx = 0UL; sliceIdx < numberOfSlices; ++sliceIdx) {
            auto numberOfBuffers = readFromMetadata();

            const auto spanStart = buffers.data() + buffIdx;
            auto recreatedSlice = deserializeSlice(std::span<const Runtime::TupleBuffer>(spanStart, numberOfBuffers));

            auto indexToInsert = std::find_if(listSlice.begin(),
                                              listSlice.end(),
                                              [&recreatedSlice](const std::unique_ptr<KeyedSlice>& currSlice) {
                                                  return recreatedSlice->getStart() > currSlice->getEnd();
                                              });
            listSlice.emplace(indexToInsert, std::move(recreatedSlice));

            buffIdx += numberOfBuffers;
        }
        sliceStorePtr->replaceSlices(std::move(listSlice));
    }
}

std::vector<Runtime::TupleBuffer> KeyedSlicePreAggregationHandler::getWatermarksToMigrate() {
    auto watermarksBuffers = std::vector<Runtime::TupleBuffer>();

    auto buffer = bufferManager->getBufferBlocking();
    watermarksBuffers.emplace_back(buffer);
    uint64_t buffersCount = 1;

    if (!buffer.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    auto bufferPtr = buffer.getBuffer<uint64_t>();
    uint64_t bufferIdx = 0;

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

    writeToBuffer(serializedWatermarks.size());

    watermarksBuffers.insert(watermarksBuffers.end(), serializedWatermarks.begin(), serializedWatermarks.end());

    for (auto& buffer : watermarksBuffers) {
        buffer.setSequenceNumber(++resultSequenceNumber);
    }

    return watermarksBuffers;
}

void KeyedSlicePreAggregationHandler::restoreWatermarks(std::vector<Runtime::TupleBuffer>& buffers) {
    uint64_t dataBuffersIdx = 0;
    auto dataPtr = buffers[dataBuffersIdx].getBuffer<uint64_t>();
    uint64_t dataIdx = 0;

    auto deserializeNextValue = [&dataPtr, &dataIdx, &dataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(dataPtr, dataIdx, dataBuffersIdx, buffers);
    };

    auto numberOfBuildBuffers = deserializeNextValue();
    watermarkProcessor->restoreWatermarks(
        std::span<const Runtime::TupleBuffer>(buffers.data() + dataBuffersIdx + 1, numberOfBuildBuffers));
}

void KeyedSlicePreAggregationHandler::setBufferManager(const NES::Runtime::BufferManagerPtr& bufManager) {
    this->bufferManager = bufManager;
}

uint64_t KeyedSlicePreAggregationHandler::getCurrentWatermark() const {
    return watermarkProcessor->getCurrentWatermark();
}
KeyedSlicePreAggregationHandler::~KeyedSlicePreAggregationHandler() { NES_DEBUG("~GlobalSlicePreAggregationHandler"); }

}// namespace NES::Runtime::Execution::Operators
