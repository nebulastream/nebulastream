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
    auto childBufferIndex = getHashMapChildBufferIndex(pos);
    if (childBufferIndex == TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        return nullptr;
    }
    auto childBuffer = loadHashMapBuffer(childBufferIndex);
    return reinterpret_cast<HashMap*>(childBuffer.getAvailableMemoryArea<>().data());
}

// AggregationSliceHashMap* AggregationSlice::getHashMapPtrOrCreate(AbstractBufferProvider* bufferProvider, const WorkerThreadId workerThreadId)
// {
//     const auto pos = workerThreadId % numberOfHashMaps();
//     INVARIANT(pos < numberOfHashMaps(), "The worker thread id should be smaller than the number of hashmaps");
//
//     auto childBufferIndex = getHashMapChildBufferIndex(pos);
//     // if (childBufferIndex == TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
//     // {
//     //     // create child buffer for this worker
//     //     auto childBuffer = bufferProvider->getBufferBlocking();
//     //     new (childBuffer.getAvailableMemoryArea<>().data()) ChainedHashMap(bufferProvider,
//     //         createNewHashMapSliceArgs.keySize,
//     //         createNewHashMapSliceArgs.valueSize,
//     //         createNewHashMapSliceArgs.numberOfBuckets,
//     //         createNewHashMapSliceArgs.pageSize);
//     //     // store child buffer into the main buffer
//     //     // todo: there is a problem here because this is not thread safe.
//     //     // multiple threads are storing child buffers at the same time.
//     //     // todo: fix
//     //     childBufferIndex = setHashMapBuffer(childBuffer, pos);
//     // }
//     // return pointer to the hash map object
//     auto childBuffer = loadHashMapBuffer(childBufferIndex);
//     return reinterpret_cast<HashMap*>(childBuffer.getAvailableMemoryArea<>().data());
// }

}
