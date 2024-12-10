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
#include <Util/SliceCache/LeastRecentlyUsedSliceCache.hpp>

namespace NES::Runtime::Execution::Operators {

LeastRecentlyUsedSliceCache::LeastRecentlyUsedSliceCache(uint64_t cacheSize) : cacheSize(cacheSize) {
    cache.wlock()->reserve(cacheSize);
}

LeastRecentlyUsedSliceCache::~LeastRecentlyUsedSliceCache() {}

std::optional<SliceCache::SlicePtr> LeastRecentlyUsedSliceCache::getSliceFromCache(uint64_t sliceId) {
    // Search for corresponding slice
    auto [cacheLocked, lruSlicesLocked] = folly::acquireLocked(cache, lruSlices);
    auto it = cacheLocked->find(sliceId);
    if (it!=cacheLocked->end()) {
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

bool LeastRecentlyUsedSliceCache::passSliceToCache(uint64_t sliceId, SliceCache::SlicePtr newSlice) {
    // Check if slice already in cache
    auto [cacheLocked, lruSlicesLocked] = folly::acquireLocked(cache, lruSlices);
    if (cacheLocked->contains(sliceId)) {
        return false;
    }
    // Check if cache full
    if(cacheLocked->size()==cacheSize) {
        // If full, remove least recently used slice
        cacheLocked->erase(lruSlicesLocked->back());
        lruSlicesLocked->pop_back();
    }
    // Add new slice to cache
    lruSlicesLocked->push_front(sliceId);
    cacheLocked->emplace(sliceId, std::make_tuple(lruSlicesLocked->begin(), newSlice));
    return true;
}
}// namespace NES::Runtime::Execution::Operators