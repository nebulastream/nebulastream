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
#include <ErrorHandling.hpp>

namespace NES
{

HashMapSlice::HashMapSlice(
    AbstractBufferProvider* bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps,
    const uint64_t numberOfInputStreams)
    : Slice(sliceStart, sliceEnd)
    , createNewHashMapSliceArgs(createNewHashMapSliceArgs)
    , numberOfHashMapsPerInputStream(numberOfHashMaps)
    , numberOfInputStreams(numberOfInputStreams)
    , workerAddressMap(numberOfHashMaps * numberOfInputStreams)
{
    if (auto buffer = bufferProvider->getUnpooledBuffer(workerAddressMap.calculateHeaderSize()))
    {
        workerAddressMap.mainBuffer = std::move(buffer.value());
        auto basePointer = workerAddressMap.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
        for (uint32_t i = 0; i < workerAddressMap.hashMapCount; i++)
        {
            basePointer[i] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
            // // create child buffer for this worker
            // auto childBuffer = bufferProvider->getBufferBlocking();
            // // create a chained hash map into the child buffer
            // auto childBufferMemoryArea = childBuffer.getAvailableMemoryArea<ChainedHashMap*>();
            // childBufferMemoryArea[0] = new ChainedHashMap(bufferProvider,
            //     createNewHashMapSliceArgs.keySize,
            //     createNewHashMapSliceArgs.valueSize,
            //     createNewHashMapSliceArgs.numberOfBuckets,
            //     createNewHashMapSliceArgs.pageSize);
            // // store child buffer into the main buffer
            // const auto childBufferIndex = workerAddressMap.mainBuffer.storeChildBuffer(childBuffer);
            // // store the childbufferindex into the mainBuffer header file
            // basePointer[i] = childBufferIndex;
        }
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

size_t HashMapSlice::WorkerAddressMap::calculateHeaderSize() const
{
    return hashMapCount * sizeof(VariableSizedAccess::Index);
}

uint64_t HashMapSlice::getNumberOfHashMaps() const
{
    return workerAddressMap.hashMapCount;
}

uint64_t HashMapSlice::getNumberOfTuples() const
{
    uint64_t runningSum = 0;
    auto bufferMemoryArea = workerAddressMap.mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    for (uint64_t i = 0; i < workerAddressMap.hashMapCount; i++)
    {
        auto childBuffer = workerAddressMap.mainBuffer.loadChildBuffer(bufferMemoryArea[i]);
        runningSum += childBuffer.getNumberOfTuples();
    }
    return runningSum;
}
}
