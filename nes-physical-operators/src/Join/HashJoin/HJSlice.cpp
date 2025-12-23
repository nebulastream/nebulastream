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
#include <Join/HashJoin/HJSlice.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{
HJSlice::HJSlice(
    AbstractBufferProvider* bufferProvider, SliceStart sliceStart, SliceEnd sliceEnd, const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs, const uint64_t numberOfHashMaps)
    : HashMapSlice(bufferProvider, std::move(sliceStart), std::move(sliceEnd), createNewHashMapSliceArgs, numberOfHashMaps, 2)
{
}

HashMap* HJSlice::getHashMapPtr(const WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    /// Hashmaps of the left build side come before right
    auto pos = (workerThreadId % numHashMapsPerInputStream())
        + ((static_cast<uint64_t>(buildSide == JoinBuildSideType::Right) * numHashMapsPerInputStream()));

    INVARIANT(
        numberOfHashMaps() > 0 and pos < numberOfHashMaps(),
        "No hashmap found for workerThreadId {} at pos {} for {} hashmaps",
        workerThreadId,
        pos,
        numberOfHashMaps());
    auto basePointer = workerAddressMap.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    if (basePointer[pos] == TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        return nullptr;
    }
    auto childBuffer = workerAddressMap.mainBuffer.loadChildBuffer(basePointer[pos]);
    return reinterpret_cast<HashMap*>(childBuffer.getAvailableMemoryArea<>().data());
}

HashMap* HJSlice::getHashMapPtrOrCreate(AbstractBufferProvider* bufferProvider, const WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide)
{
    /// Hashmaps of the left build side come before right
    auto pos = (workerThreadId % numHashMapsPerInputStream())
        + ((static_cast<uint64_t>(buildSide == JoinBuildSideType::Right) * numHashMapsPerInputStream()));

    INVARIANT(
        numberOfHashMaps() > 0 and pos < numberOfHashMaps(),
        "No hashmap found for workerThreadId {} at pos {} for {} hashmaps",
        workerThreadId,
        pos,
        numberOfHashMaps());

    auto basePointer = workerAddressMap.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    if (basePointer[pos] == TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        // create child buffer for this worker
        auto childBuffer = bufferProvider->getBufferBlocking();
        new (childBuffer.getAvailableMemoryArea<>().data()) ChainedHashMap(bufferProvider,
            createNewHashMapSliceArgs.keySize,
            createNewHashMapSliceArgs.valueSize,
            createNewHashMapSliceArgs.numberOfBuckets,
            createNewHashMapSliceArgs.pageSize);
        // store child buffer into the main buffer
        const auto childBufferIndex = workerAddressMap.mainBuffer.storeChildBuffer(childBuffer);
        // store the childbufferindex into the mainBuffer header file
        basePointer[pos] = childBufferIndex;
    }
    // return pointer to the hash map object
    auto childBuffer = workerAddressMap.mainBuffer.loadChildBuffer(basePointer[pos]);
    return reinterpret_cast<HashMap*>(childBuffer.getAvailableMemoryArea<>().data());
}

uint64_t HJSlice::getNumberOfHashMapsForSide() const
{
    return numHashMapsPerInputStream();
}

}
