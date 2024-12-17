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
#include <Util/SliceCache/LeastRecentlyUsedSliceCacheWithLock.hpp>

namespace NES::Runtime::Execution::Operators
{

LeastRecentlyUsedSliceCacheWithLock::LeastRecentlyUsedSliceCacheWithLock(uint64_t cacheSize, SliceAssigner sliceAssigner)
    : cacheSize(cacheSize), sliceAssigner(sliceAssigner)
{
}

LeastRecentlyUsedSliceCacheWithLock::LeastRecentlyUsedSliceCacheWithLock(LeastRecentlyUsedSliceCacheWithLock const& other)
    : cacheSize(other.cacheSize), sliceAssigner(other.sliceAssigner)
{
}

LeastRecentlyUsedSliceCacheWithLock::~LeastRecentlyUsedSliceCacheWithLock()
{
}

SliceCachePtr LeastRecentlyUsedSliceCacheWithLock::clone() const
{
    return std::make_shared<LeastRecentlyUsedSliceCacheWithLock>(*this);
}

std::optional<SliceCache::SlicePtr> LeastRecentlyUsedSliceCacheWithLock::getSliceFromCache(Timestamp timestamp)
{
    // Get slice end as a unique sliceId
    auto sliceId = sliceAssigner.getSliceEndTs(timestamp).getRawValue();

    // Search for corresponding slice
    auto [cacheLocked, lruSlicesLocked] = folly::acquireLocked(cache, lruSlices);
    auto it = cacheLocked->find(sliceId);
    if (it != cacheLocked->end())
    {
        // If found, bring slice to front of cache and return
        auto& currentTuple = it->second;
        const auto oldListPosition = std::get<listPosition>(currentTuple);

        lruSlicesLocked->erase(oldListPosition);

        lruSlicesLocked->push_front(sliceId);
        std::get<listPosition>(currentTuple) = lruSlicesLocked->begin();

        return std::get<SliceCache::SlicePtr>(currentTuple);
    }
    // If not found, return nullopt
    return {};
}

bool LeastRecentlyUsedSliceCacheWithLock::passSliceToCache(Timestamp sliceId, SliceCache::SlicePtr newSlice)
{
    // Get the value of sliceId
    auto sliceIdValue = sliceId.getRawValue();

    // Check if slice already in cache
    auto [cacheLocked, lruSlicesLocked] = folly::acquireLocked(cache, lruSlices);
    if (cacheLocked->contains(sliceIdValue))
    {
        return false;
    }
    // Check if cache full
    if (cacheLocked->size() == cacheSize)
    {
        // If full, remove least recently used slice
        cacheLocked->erase(lruSlicesLocked->back());
        lruSlicesLocked->pop_back();
    }
    // Add new slice to cache
    lruSlicesLocked->push_front(sliceIdValue);
    cacheLocked->emplace(sliceIdValue, std::make_tuple(lruSlicesLocked->begin(), newSlice));
    return true;
}

void LeastRecentlyUsedSliceCacheWithLock::deleteSlicesFromCache(std::vector<Timestamp> slicesToDelete)
{
    auto [cacheLocked, lruSlicesLocked] = folly::acquireLocked(cache, lruSlices);

    for (const auto& sliceIdToDelete : slicesToDelete)
    {
        // Get the value of sliceId
        auto sliceId = sliceIdToDelete.getRawValue();
        auto it = cacheLocked->find(sliceId);
        if (it == cacheLocked->end())
        {
            // If slice is not found, continue
            continue;
        }
        // If found, delete slice
        const auto oldListPosition = std::get<listPosition>(it->second);
        lruSlicesLocked->erase(oldListPosition);
        cacheLocked->erase(it);
    }
}

} // namespace NES::Runtime::Execution::Operators