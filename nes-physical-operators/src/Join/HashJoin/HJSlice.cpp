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
    SliceStart sliceStart, SliceEnd sliceEnd, CreateNewHashMapSliceArgs createNewHashMapSliceArgs, const uint64_t numberOfHashMaps)
    : HashMapSlice(std::move(sliceStart), std::move(sliceEnd), std::move(createNewHashMapSliceArgs), numberOfHashMaps, 2)
{
}

Nautilus::Interface::HashMap* HJSlice::getHashMapPtr(const WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    /// Hashmaps of the left build side come before right
    auto pos = (workerThreadId % numberOfHashMapsPerInputStream)
        + (static_cast<uint64_t>((buildSide == JoinBuildSideType::Right) * numberOfHashMapsPerInputStream));

    INVARIANT(
        not hashMaps.empty() and pos < hashMaps.size(),
        "No hashmap found for workerThreadId {} and pos {} for {} hashmaps",
        workerThreadId,
        pos,
        hashMaps.size());
    return hashMaps[pos].get();
}

Nautilus::Interface::HashMap* HJSlice::getHashMapPtrOrCreate(const WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide)
{
    /// Hashmaps of the left build side come before right
    auto pos = (workerThreadId % numberOfHashMapsPerInputStream)
        + (static_cast<uint64_t>((buildSide == JoinBuildSideType::Right) * numberOfHashMapsPerInputStream));

    INVARIANT(
        not hashMaps.empty() and pos < hashMaps.size(),
        "No hashmap found for workerThreadId {} and pos {} for {} hashmaps",
        workerThreadId,
        pos,
        hashMaps.size());

    if (hashMaps.at(pos) == nullptr)
    {
        /// Hashmap at pos has not been initialized
        hashMaps.at(pos) = std::make_unique<Nautilus::Interface::ChainedHashMap>(
            createNewHashMapSliceArgs.keySize,
            createNewHashMapSliceArgs.valueSize,
            createNewHashMapSliceArgs.numberOfBuckets,
            createNewHashMapSliceArgs.pageSize);
    }
    return hashMaps.at(pos).get();
}

uint64_t HJSlice::getNumberOfHashMapsForSide() const
{
    return numberOfHashMapsPerInputStream;
}

}
