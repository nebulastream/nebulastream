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
#include <Util/SliceCache/FIFOSliceCache.hpp>

namespace NES::Runtime::Execution::Operators {

FIFOSliceCache::FIFOSliceCache(uint64_t cacheSize) : cacheSize(cacheSize) {
    cache.wlock()->reserve(cacheSize);
}

FIFOSliceCache::~FIFOSliceCache() {}

std::optional<SliceCache::SlicePtr> FIFOSliceCache::getSliceFromCache(uint64_t sliceId) {
    // Get read lock for cache
    auto cacheLocked = cache.rlock();

    // Search for corresponding slice
    auto it = cacheLocked->find(sliceId);

    if (it == cacheLocked->end()) {
        // If slice is not found, return nullopt
        return {};
    }

    // Return pointer to the slice
    return cacheLocked->find(sliceId)->second;
}

bool FIFOSliceCache::passSliceToCache(uint64_t sliceId, SliceCache::SlicePtr newSlice) {
    {
        // Get read lock for cache
        auto cacheLocked = cache.rlock();

        // Check if slice is already in cache
        if (cacheLocked->contains(sliceId)) {
            return false;
        }
    }
    
    // Get write lock for cache and slices
    auto [cacheWLocked, slicesLocked] = folly::acquireLocked(cache, slices);

    // Check if cache is full
    if (cacheWLocked->size() == cacheSize) {
        // If full, remove last slice in queue
        cacheWLocked->erase(slicesLocked->back());
        slicesLocked->pop_back();
    }
    // Add new slice to cache
    slicesLocked->push_front(sliceId);
    cacheWLocked->emplace(sliceId, newSlice);
        
    return true;
}
}// namespace NES::Runtime::Execution::Operators