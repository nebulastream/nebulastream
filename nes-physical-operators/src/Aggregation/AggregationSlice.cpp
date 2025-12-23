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
#include <Aggregation/AggregationSlice.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <spdlog/spdlog.h>

#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{
AggregationSlice::AggregationSlice(
    AbstractBufferProvider* bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps)
    : HashMapSlice(bufferProvider, sliceStart, sliceEnd, createNewHashMapSliceArgs, numberOfHashMaps, 1)
{
}

HashMap* AggregationSlice::getHashMapPtr(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % numberOfHashMaps();
    INVARIANT(pos < numberOfHashMaps(), "The worker thread id should be smaller than the number of hashmaps");
    auto basePointer = workerAddressMap.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    if (basePointer[pos] == TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        return nullptr;
    }
    auto childBuffer = workerAddressMap.mainBuffer.loadChildBuffer(basePointer[pos]);
    return reinterpret_cast<HashMap*>(childBuffer.getAvailableMemoryArea<>().data());
}

HashMap* AggregationSlice::getHashMapPtrOrCreate(AbstractBufferProvider* bufferProvider, const WorkerThreadId workerThreadId)
{
    const auto pos = workerThreadId % numberOfHashMaps();
    INVARIANT(pos < numberOfHashMaps(), "The worker thread id should be smaller than the number of hashmaps");

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

}
