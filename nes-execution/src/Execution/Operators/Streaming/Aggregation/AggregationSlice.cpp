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
#include <Execution/Operators/Streaming/Aggregation/AggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMapInterface.hpp>

namespace NES::Runtime::Execution
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

Nautilus::Interface::HashMapInterface* AggregationSlice::getHashMapPtr(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % hashMaps.size();
    return hashMaps[pos].get();
}

uint64_t AggregationSlice::getNumberOfHashMaps() const
{
    return hashMaps.size();
}

uint64_t AggregationSlice::getNumberOfTuples() const
{
    return std::accumulate(
        hashMaps.begin(), hashMaps.end(), 0UL, [](uint64_t sum, const auto& hashMap) { return sum + hashMap->getNumberOfTuples(); });
}

}
