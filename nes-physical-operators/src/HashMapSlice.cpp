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
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

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
}
