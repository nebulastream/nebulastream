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
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

HashMapSlice::HashMapSlice(const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfHashMaps)
    : Slice(sliceStart, sliceEnd), numberOfHashMaps(numberOfHashMaps)
{
    for (uint64_t i = 0; i < numberOfHashMaps; i++)
    {
        hashMaps.emplace_back(nullptr);
    }
}

HashMapSlice::~HashMapSlice()
{
    /// Removing all hashmaps that have no seen tuples. We do not need to call the cleanup function for them
    std::erase_if(
        hashMaps,
        [](const std::unique_ptr<Nautilus::Interface::HashMap>& curHashMap)
        { return curHashMap and curHashMap->getNumberOfTuples() == 0; });

    /// If there exist one hashmap with at least 1 tuple, we expect the cleanup function to be set
    if (not cleanupFunction)
    {
        const bool allNullptr = std::ranges::all_of(hashMaps, [](const auto& ptr) { return ptr == nullptr; });
        INVARIANT(allNullptr, "The cleanup function should be set before the slice is destroyed. Have you called setCleanupFunction?");
    }
    else
    {
        /// Invoking the cleanup function
        cleanupFunction(hashMaps);
    }

    hashMaps.clear();
}

Nautilus::Interface::HashMap* HashMapSlice::getHashMapPtr(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % hashMaps.size();
    INVARIANT(pos < hashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
    return hashMaps[pos].get();
}

Nautilus::Interface::HashMap*
HashMapSlice::getHashMapPtrOrCreate(const WorkerThreadId workerThreadId, const CreateNewHashMapSliceArgs& hashMapArgs)
{
    const auto pos = workerThreadId % hashMaps.size();
    INVARIANT(pos < hashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");

    if (hashMaps.at(pos) == nullptr)
    {
        PRECONDITION(
            hashMapArgs.keySize.has_value() and hashMapArgs.valueSize.has_value() and hashMapArgs.numberOfBuckets.has_value()
                and hashMapArgs.pageSize.has_value(),
            "Keysize, valueSize, numberOfBuckets, and pageSize MUST BE set before creating a new HashMapSlice!");

        hashMaps.at(pos) = std::make_unique<Nautilus::Interface::ChainedHashMap>(
            hashMapArgs.keySize.value(), hashMapArgs.valueSize.value(), hashMapArgs.numberOfBuckets.value(), hashMapArgs.pageSize.value());
    }
    return hashMaps[pos].get();
}

uint64_t HashMapSlice::getNumberOfHashMaps() const
{
    return hashMaps.size();
}

uint64_t HashMapSlice::getNumberOfTuples() const
{
    return std::accumulate(
        hashMaps.begin(),
        hashMaps.end(),
        0,
        [](uint64_t runningSum, const auto& hashMap) { return runningSum + hashMap->getNumberOfTuples(); });
}

void HashMapSlice::setCleanupFunction(
    const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction)
{
    if (this->cleanupFunction == nullptr)
    {
        PRECONDITION(
            cleanupFunction != nullptr, "The cleanup function should not be null. Have you passed a null function to setCleanupFunction?");
        this->cleanupFunction = cleanupFunction;
    }
}

}
