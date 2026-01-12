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
#include <HashMapSlice.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <antlr4-runtime/antlr4-common.h>
#include <google/protobuf/stubs/port.h>

#include <ErrorHandling.hpp>

namespace NES
{

HashMapSlice::HashMapDirectory::HashMapDirectory(AbstractBufferProvider* bufferProvider,
    const uint64_t numHashMaps,
    const uint64_t numInputStreams,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs)
{
    if (auto buffer = bufferProvider->getUnpooledBuffer(calculateMainBufferSize(numHashMaps, numInputStreams)))
    {
        mainBuffer = std::move(buffer.value());
        auto metadataPointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
        metadataPointer[Offsets::NUM_HASH_MAPS_POS] = numHashMaps * numInputStreams;
        metadataPointer[Offsets::NUM_INPUT_STREAMS_POS] = numInputStreams;
        metadataPointer[Offsets::NUM_HASH_MAPS_PER_INPUT_STREAM_POS] = numHashMaps;

        auto hashMapsPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
        for (uint32_t i = 0; i < metadataPointer[Offsets::NUM_HASH_MAPS_POS]; i++)
        {
            // create child buffer for each hashmap and an empty hash map in each
            auto childBuffer = bufferProvider->getBufferBlocking();
            new (childBuffer.getAvailableMemoryArea<>().data()) ChainedHashMap(bufferProvider,
                createNewHashMapSliceArgs.keySize,
                createNewHashMapSliceArgs.valueSize,
                createNewHashMapSliceArgs.numberOfBuckets,
                createNewHashMapSliceArgs.pageSize);
            auto childBufferIndex = mainBuffer.storeChildBuffer(childBuffer);
            hashMapsPointer[Offsets::HASH_MAPS_BEGIN_POS + i] = childBufferIndex;
        }
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

size_t HashMapSlice::HashMapDirectory::calculateMainBufferSize(const uint64_t numHashMaps, const uint64_t numInputStreams) const
{
    return 3 * sizeof(uint64_t) + (1 + numHashMaps * numInputStreams) * sizeof(VariableSizedAccess::Index);
}

VariableSizedAccess::Index HashMapSlice::getHashMapChildBufferIndex(const uint64_t pos) const
{
    return hashMapDirectory.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[HashMapDirectory::Offsets::HASH_MAPS_BEGIN_POS + pos];
}

TupleBuffer HashMapSlice::loadHashMapBuffer(const VariableSizedAccess::Index childBufferIndex) const
{
    TupleBuffer childBuffer = hashMapDirectory.mainBuffer.loadChildBuffer(childBufferIndex);
    return childBuffer;
}

VariableSizedAccess::Index HashMapSlice::setHashMapBuffer(TupleBuffer hashMapBuffer, const uint64_t pos)
{
    auto childBufferIndex = hashMapDirectory.mainBuffer.storeChildBuffer(hashMapBuffer);
    auto hashMapsPointer = hashMapDirectory.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    hashMapsPointer[HashMapDirectory::Offsets::HASH_MAPS_BEGIN_POS + pos] = childBufferIndex;
    return childBufferIndex;
}

uint64_t HashMapSlice::numberOfHashMaps() const
{
    return hashMapDirectory.mainBuffer.getAvailableMemoryArea<uint64_t>()[HashMapDirectory::Offsets::NUM_HASH_MAPS_POS];
}

uint64_t HashMapSlice::numInputStreams() const
{
    return hashMapDirectory.mainBuffer.getAvailableMemoryArea<uint64_t>()[HashMapDirectory::Offsets::NUM_INPUT_STREAMS_POS];
}

uint64_t HashMapSlice::numHashMapsPerInputStream() const
{
    return hashMapDirectory.mainBuffer.getAvailableMemoryArea<uint64_t>()[HashMapDirectory::Offsets::NUM_HASH_MAPS_PER_INPUT_STREAM_POS];
}

uint64_t HashMapSlice::getNumberOfTuples() const
{
    uint64_t runningSum = 0;
    auto bufferMemoryArea = hashMapDirectory.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    for (uint64_t i = 0; i < numberOfHashMaps(); i++)
    {
        auto childBuffer = hashMapDirectory.mainBuffer.loadChildBuffer(bufferMemoryArea[i]);
        runningSum += childBuffer.getNumberOfTuples();
    }
    return runningSum;
}
}
