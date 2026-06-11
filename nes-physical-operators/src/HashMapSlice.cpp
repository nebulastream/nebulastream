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
#include <Runtime/VariableSizedAccess.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

HashMapSlice::HashMapSlice(
    AbstractBufferProvider& bufferProvider,
    SliceStart sliceStart,
    SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps,
    const uint64_t numberOfInputStreams)
    : Slice(sliceStart, sliceEnd)
    , numHashMaps(numberOfHashMaps * numberOfInputStreams)
    , numInputStreams(numberOfInputStreams)
    , numHashmapsPerInputStream(numberOfHashMaps)
    , params(createNewHashMapSliceArgs)
{
    /// initialize chained hash map buffers
    hashMapBuffers.reserve(numHashMaps);
    /// allocate buffers for the hash maps
    for (uint32_t i = 0; i < numHashMaps; i++)
    {
        if (auto childBuffer
            = bufferProvider.getUnpooledBuffer(ChainedHashMap::calculateBufferSizeFromBuckets(createNewHashMapSliceArgs.numberOfBuckets)))
        {
            /// initialize chained hash map i
            const ChainedHashMap chm = ChainedHashMap::init(
                childBuffer.value(),
                createNewHashMapSliceArgs.keySize,
                createNewHashMapSliceArgs.valueSize,
                createNewHashMapSliceArgs.numberOfBuckets,
                createNewHashMapSliceArgs.pageSize);
            hashMapBuffers.emplace_back(childBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map child buffer!");
        }
    }
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

const TupleBuffer* HashMapSlice::getHashMapBufferRef(const VariableSizedAccess::Index childBufferIndex) const
{
    PRECONDITION(childBufferIndex.getRawIndex() < numHashMaps, "Hash Map index out of range in hash map slice loadHashMapBuffer!");
    return &hashMapBuffers[childBufferIndex.getRawIndex()];
}
}
