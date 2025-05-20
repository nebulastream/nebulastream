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

#include <cstdint>
#include <memory>
#include <numeric>
#include <Aggregation/AggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AggregationSlice::AggregationSlice(
    const uint64_t keySize,
    const uint64_t valueSize,
    const uint64_t numberOfBuckets,
    const uint64_t pageSize,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numberOfHashMaps)
    : Slice(sliceStart, sliceEnd)
{
    for (uint64_t i = 0; i < numberOfHashMaps; ++i)
    {
        hashMaps.emplace_back(std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfBuckets, pageSize));
    }
}

AggregationSlice::~AggregationSlice()
{
    PRECONDITION(
        cleanupFunction != nullptr,
        "The cleanup function should be set before the slice is destroyed. Have you called setCleanupFunction?");
    cleanupFunction(hashMaps);
    hashMaps.clear();
}

Nautilus::Interface::HashMap* AggregationSlice::getHashMapPtr(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % hashMaps.size();
    INVARIANT(pos < hashMaps.size(), "The worker thread id should be smaller than the number of hashmaps");
    return hashMaps[pos].get();
}

uint64_t AggregationSlice::getNumberOfHashMaps() const
{
    return hashMaps.size();
}

uint64_t AggregationSlice::getNumberOfTuples() const
{
    return std::accumulate(
        hashMaps.begin(),
        hashMaps.end(),
        0,
        [](uint64_t runningSum, const auto& hashMap) { return runningSum + hashMap->getNumberOfTuples(); });
}

void AggregationSlice::setCleanupFunction(
    const std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)>& cleanupFunction)
{
    if (this->cleanupFunction == nullptr)
    {
        PRECONDITION(
            cleanupFunction != nullptr, "The cleanup function should not be null. Have you passed a null function to setCleanupFunction?");
        this->cleanupFunction = std::move(cleanupFunction);
    }
}

}
