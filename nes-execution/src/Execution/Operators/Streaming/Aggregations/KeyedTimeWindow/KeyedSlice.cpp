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
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

KeyedSlice::KeyedSlice(std::unique_ptr<Nautilus::Interface::ChainedHashMap> hashMap, uint64_t start, uint64_t end)
    : start(start), end(end), state(std::move(hashMap)) {}

KeyedSlice::~KeyedSlice() { NES_DEBUG("~KeyedSlice {}-{}", start, end); }

std::vector<Runtime::TupleBuffer> KeyedSlice::serialize(BufferManagerPtr& bufferManager) {
    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    auto mainMetadata = bufferManager->getBufferBlocking();
    buffersToTransfer.emplace(buffersToTransfer.begin(), mainMetadata);
    uint64_t metadataBuffersCount = 1;

    // check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!mainMetadata.hasSpaceLeft(0, sizeof(uint64_t))) {
        NES_THROW_RUNTIME_ERROR("Buffer is too small to store metadata.");
    }

    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    uint64_t metadataIdx = 1;

    /** @brief Lambda to write to metadata buffers */
    auto writeToMetadata = [&mainMetadata, &metadataPtr, &metadataIdx, &bufferManager, &metadataBuffersCount, &buffersToTransfer](uint64_t dataToWrite) {
        StateSerializationUtil::writeToBuffer(bufferManager,
                                              mainMetadata.getBufferSize(),
                                              metadataPtr,
                                              metadataIdx,
                                              metadataBuffersCount,
                                              buffersToTransfer,
                                              dataToWrite);
    };

    // Store slice metadata
    writeToMetadata(start);
    writeToMetadata(end);
    writeToMetadata(state->getCurrentSize());

    // Extract and store ChainedHashMap contents
    if (state) {
        auto stateSerialized = state->serialize();

        auto stateBuffer = bufferManager->getBufferBlocking();
        if (stateBuffer.getBufferSize() < stateSerialized.size()) {
            NES_THROW_RUNTIME_ERROR("Allocated buffer is too small for serialized state.");
        }

        std::memcpy(stateBuffer.getBuffer<uint8_t>(), stateSerialized.data(), stateSerialized.size());
        buffersToTransfer.emplace_back(stateBuffer);
    }

    // set number of metadata buffers to the first metadata buffer
    mainMetadata.getBuffer<uint64_t>()[0] = metadataBuffersCount;

    return buffersToTransfer;
}

KeyedSlicePtr KeyedSlice::deserialize(std::span<const Runtime::TupleBuffer> buffers) {
    uint64_t metadataBuffersIdx = 0;
    auto metadataPtr = buffers[metadataBuffersIdx++].getBuffer<uint64_t>();
    auto hashMapData = buffers[metadataBuffersIdx].getBuffer<uint8_t>();
    uint64_t metadataIdx = 0;

    // read metadata buffer count
    auto numberOfMetadataBuffers = metadataPtr[metadataIdx++];

    /** @brief Lambda to read from metadata */
    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t {
        return StateSerializationUtil::readFromBuffer(metadataPtr, metadataIdx, metadataBuffersIdx, buffers);
    };

    // Read slice metadata
    uint64_t sliceStart = readFromMetadata();
    uint64_t sliceEnd = readFromMetadata();
    uint64_t hashMapSize = readFromMetadata();

    auto allocator = std::make_unique<std::pmr::monotonic_buffer_resource>();
    std::unique_ptr<Nautilus::Interface::ChainedHashMap> hashMap = Nautilus::Interface::ChainedHashMap::deserialize(hashMapData, hashMapSize, std::unique_ptr<std::pmr::memory_resource>(std::move(allocator)));

    auto newSlice = std::make_unique<KeyedSlice>(std::move(hashMap), sliceStart, sliceEnd);

    return newSlice;
}
}// namespace NES::Runtime::Execution::Operators
