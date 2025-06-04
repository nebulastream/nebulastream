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
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{
AggregationSlice::AggregationSlice(
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    CreateNewHashMapSliceArgs createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps)
    : HashMapSlice(sliceStart, sliceEnd, std::move(createNewHashMapSliceArgs), numberOfHashMaps, 1)
{
}


Nautilus::Interface::HashMap* AggregationSlice::getHashMapPtr(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % hashMaps.size();
    INVARIANT(pos < hashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
    return hashMaps[pos].get();
}

Nautilus::Interface::HashMap* AggregationSlice::getHashMapPtrOrCreate(const WorkerThreadId workerThreadId)
{
    const auto pos = workerThreadId % hashMaps.size();
    INVARIANT(pos < hashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");

    if (hashMaps.at(pos) == nullptr)
    {
        hashMaps.at(pos) = std::make_unique<Nautilus::Interface::ChainedHashMap>(
            createNewHashMapSliceArgs.keySize,
            createNewHashMapSliceArgs.valueSize,
            createNewHashMapSliceArgs.numberOfBuckets,
            createNewHashMapSliceArgs.pageSize);
    }
    return hashMaps[pos].get();
}

}
