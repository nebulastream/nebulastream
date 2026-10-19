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
#include <istream>
#include <memory>
#include <numeric>
#include <ostream>
#include <span>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
void writeValue(std::ostream& out, const uint64_t value)
{
    out.write(reinterpret_cast<const char*>(&value), sizeof(value)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (!out)
    {
        throw CannotSerialize("Failed to write hash map slice");
    }
}

uint64_t readValue(std::istream& in)
{
    uint64_t value = 0;
    in.read(reinterpret_cast<char*>(&value), sizeof(value)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (!in)
    {
        throw CannotDeserialize("Truncated hash map slice");
    }
    return value;
}
}

HashMapSlice::HashMapSlice(
    SliceStart sliceStart,
    SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps,
    const uint64_t numberOfInputStreams)
    : Slice(sliceStart, sliceEnd)
    , numHashMaps(numberOfHashMaps * numberOfInputStreams)
    , numInputStreams(numberOfInputStreams)
    , numHashmapsPerInputStream(numberOfHashMaps)
    , hashMapConfig(createNewHashMapSliceArgs.config)
{
    hashMapBuffers.resize(numHashMaps);
    hashMapBuffersState.assign(numHashMaps, HashMapBufferState::UNINITIALIZED);
}

uint64_t HashMapSlice::getNumberOfHashMaps() const
{
    return numHashMaps;
}

uint64_t HashMapSlice::getNumInputStreams() const
{
    return numInputStreams;
}

uint64_t HashMapSlice::getNumHashMapsPerInputStream() const
{
    return numHashmapsPerInputStream;
}

const TupleBuffer* HashMapSlice::getHashMapBufferRef(const ChildBufferIndex childBufferIndex) const
{
    PRECONDITION(childBufferIndex.getRawValue() < numHashMaps, "Hash Map index out of range in hash map slice getHashMapBufferRef!");
    const auto pos = childBufferIndex.getRawValue();
    return hashMapBuffersState[pos] == HashMapBufferState::INITIALIZED ? &hashMapBuffers[pos] : nullptr;
}

const TupleBuffer*
HashMapSlice::getOrCreateHashMapBufferRef(AbstractBufferProvider& bufferProvider, const ChildBufferIndex childBufferIndex)
{
    PRECONDITION(childBufferIndex.getRawValue() < numHashMaps, "Hash Map index out of range in hash map slice loadHashMapBuffer!");
    const auto pos = childBufferIndex.getRawValue();
    /// Check whether the hash map is initialized
    if (hashMapBuffersState[pos] == HashMapBufferState::UNINITIALIZED)
    {
        /// initialize the chained hash map buffer
        /// allocate buffers for the hash maps
        if (auto childBuffer = bufferProvider.getUnpooledBuffer(
                ChainedHashMap::calculateBufferSize(hashMapConfig.numberOfBuckets, hashMapConfig.bloomFilterMemAreaSize())))
        {
            /// initialize chained hash map i
            ChainedHashMap::init(childBuffer.value(), hashMapConfig);
            hashMapBuffers[pos] = childBuffer.value();
            hashMapBuffersState[pos] = HashMapBufferState::INITIALIZED;
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map child buffer!");
        }
    }
    return &hashMapBuffers[pos];
}

void HashMapSlice::serializeHashMaps(std::ostream& out, const std::span<const ChainedHashMapConfig> configPerInputStream) const
{
    PRECONDITION(
        configPerInputStream.size() == numInputStreams,
        "Expected one hash map config per input stream, but got {} for {} input streams",
        configPerInputStream.size(),
        numInputStreams);
    std::vector<uint64_t> createdHashMaps;
    for (uint64_t position = 0; position < numHashMaps; ++position)
    {
        if (hashMapBuffersState[position] == HashMapBufferState::INITIALIZED)
        {
            createdHashMaps.push_back(position);
        }
    }

    writeValue(out, numHashmapsPerInputStream);
    writeValue(out, createdHashMaps.size());
    for (const auto position : createdHashMaps)
    {
        writeValue(out, position);
        ChainedHashMap::load(hashMapBuffers[position]).serialize(out, configPerInputStream[position / numHashmapsPerInputStream]);
    }
}

void HashMapSlice::deserializeHashMaps(
    std::istream& in, const std::span<const ChainedHashMapConfig> configPerInputStream, AbstractBufferProvider& bufferProvider)
{
    PRECONDITION(
        configPerInputStream.size() == numInputStreams,
        "Expected one hash map config per input stream, but got {} for {} input streams",
        configPerInputStream.size(),
        numInputStreams);
    if (const auto hashMapsPerInputStream = readValue(in); hashMapsPerInputStream != numHashmapsPerInputStream)
    {
        throw CannotDeserialize(
            "Slice was written with {} hash maps per input stream, but has {}. Was the number of worker threads changed?",
            hashMapsPerInputStream,
            numHashmapsPerInputStream);
    }
    const auto numberOfCreatedHashMaps = readValue(in);
    for (uint64_t hashMap = 0; hashMap < numberOfCreatedHashMaps; ++hashMap)
    {
        const auto position = readValue(in);
        if (position >= numHashMaps)
        {
            throw CannotDeserialize("Hash map position {} is out of range for a slice with {} hash maps", position, numHashMaps);
        }
        hashMapBuffers[position]
            = ChainedHashMap::deserialize(in, configPerInputStream[position / numHashmapsPerInputStream], bufferProvider);
        hashMapBuffersState[position] = HashMapBufferState::INITIALIZED;
    }
}

}
