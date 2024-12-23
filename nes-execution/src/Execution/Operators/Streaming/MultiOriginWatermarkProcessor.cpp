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
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>

namespace NES::Runtime::Execution::Operators {

MultiOriginWatermarkProcessor::MultiOriginWatermarkProcessor(const std::vector<OriginId>& origins) : origins(origins) {
    for (const auto& _ : origins) {
        watermarkProcessors.emplace_back(std::make_shared<Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>());
    }
};

std::shared_ptr<MultiOriginWatermarkProcessor> MultiOriginWatermarkProcessor::create(const std::vector<OriginId>& origins) {
    return std::make_shared<MultiOriginWatermarkProcessor>(origins);
}

// TODO use here the BufferMetaData class for the params #4177
uint64_t MultiOriginWatermarkProcessor::updateWatermark(uint64_t ts, SequenceData sequenceData, OriginId origin) {
    bool found = false;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex) {
        if (origins[originIndex] == origin) {
            watermarkProcessors[originIndex]->emplace(sequenceData, ts);
            found = true;
        }
    }
    if (!found) {
        std::stringstream ss;
        for (auto& id : origins) {
            ss << id << ",";
        }
        NES_THROW_RUNTIME_ERROR("update watermark for non existing origin " << origin << " number of origins=" << origins.size()
                                                                            << " ids=" << ss.str());
    }
    return getCurrentWatermark();
}

std::string MultiOriginWatermarkProcessor::getCurrentStatus() {
    std::stringstream ss;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex) {
        ss << " id=" << origins[originIndex] << " watermark=" << watermarkProcessors[originIndex]->getCurrentValue();
    }
    return ss.str();
}

uint64_t MultiOriginWatermarkProcessor::getCurrentWatermark() {
    auto minimalWatermark = UINT64_MAX;
    for (const auto& wt : watermarkProcessors) {
        minimalWatermark = std::min(minimalWatermark, wt->getCurrentValue());
    }
    return minimalWatermark;
}

std::vector<Runtime::TupleBuffer>
MultiOriginWatermarkProcessor::serializeWatermarks(std::shared_ptr<BufferManager> bufferManager) const {
    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    auto dataBuffer = bufferManager->getBufferBlocking();
    buffersToTransfer.emplace(buffersToTransfer.begin(), dataBuffer);
    auto dataBuffersCount = 1;

    // check that tuple buffer size is more than or equal to uint64_t
    if (!dataBuffer.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR(
            "Buffer size has to be at least greater or equal to uint64_t in size for successful state migration.");
    }
    auto dataPtr = dataBuffer.getBuffer<uint64_t>();
    auto dataIdx = 0ULL;

    // TODO: make Util function for that actually

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
        [&dataBuffer, &dataPtr, &dataIdx, &bufferManager, &dataBuffersCount, &buffersToTransfer](uint64_t dataToWrite) {
            // check that current metadata buffer has enough space, by sending used space and space needed
            if (!dataBuffer.hasSpaceLeft(dataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
                // if current buffer does not contain enough space then
                // get new buffer and insert to vector of buffers
                auto newBuffer = bufferManager->getBufferBlocking();
                // add this buffer to vector of buffers
                buffersToTransfer.emplace(buffersToTransfer.begin() + dataBuffersCount++, newBuffer);
                // reset pointer to point new buffer and reset index to 0
                dataPtr = newBuffer.getBuffer<uint64_t>();
                dataIdx = 0;
            }
            dataPtr[dataIdx++] = dataToWrite;
        };

    // NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    // write information from every watermark processor to buffers
    // 0. Write number of origins
    writeToMetadata(origins.size());
    // 1. Go over origins and write watermark processor state
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex) {
        // get information stored in watermark processor represented as state of container
        auto states = watermarkProcessors[originIndex]->serialize();
        // 2. Write number of states
        writeToMetadata(states.size());
        for (const auto& state : states) {
            // 3. Write state content
            auto seqNumber = std::get<0>(state);
            writeToMetadata(seqNumber);
            auto lastChunkNumber = std::get<1>(state);
            writeToMetadata(lastChunkNumber);
            auto seenChunks = std::get<2>(state);
            writeToMetadata(seenChunks);
            auto watermark = std::get<3>(state);
            writeToMetadata(watermark);
        }
    }

    return buffersToTransfer;
}

void MultiOriginWatermarkProcessor::restoreWatermarks(std::span<const Runtime::TupleBuffer> buffers) {
    // get first buffer
    auto dataBuffersIdx = 0;
    auto dataPtr = buffers[dataBuffersIdx].getBuffer<uint64_t>();
    auto dataIdx = 0;

    /** @brief Lambda to read from metadata buffers
     * captured variables:
     * metadataPtr - pointer to the start of current metadata buffer
     * metadataIdx - index of size64_t data inside metadata buffer
     * metadataBuffersIdx - index of current buffer to read
     * buffers - vector of buffers
    */
    auto readFromMetadata = [&dataPtr, &dataIdx, &dataBuffersIdx, &buffers]() -> uint64_t {
        // check left space in data buffer
        if (!buffers[dataBuffersIdx].hasSpaceLeft(dataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
            // if no space left inside current buffer
            // reset data pointer and reset index
            dataPtr = buffers[++dataBuffersIdx].getBuffer<uint64_t>();
            dataIdx = 0;
        }
        return dataPtr[dataIdx++];
    };
    // NOTE: Do not change the order of reads from metadata (order is documented in function declaration)
    // 0. Retrieve number of origins
    auto numberOfOrigins = readFromMetadata();

    if (numberOfOrigins != origins.size()) {
        NES_NOT_IMPLEMENTED();
    }
    // 1. Go over origin ids and recreate watermark processors
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex) {
        auto numberOfStates = readFromMetadata();
        // 2. Get number of states
        auto states = std::vector<NES::Sequencing::ContainerState<uint64_t>>();

        // 3. Get state content
        for (uint64_t stateId = 0; stateId < numberOfStates; stateId++) {
            auto seqNumber = readFromMetadata();
            auto lastChunkNumber = readFromMetadata();
            auto seenChunks = readFromMetadata();
            auto watermark = readFromMetadata();
            states.emplace_back(seqNumber, lastChunkNumber, seenChunks, watermark);
        }

        watermarkProcessors[originIndex]->deserialize(states);
    }
}

}// namespace NES::Runtime::Execution::Operators
