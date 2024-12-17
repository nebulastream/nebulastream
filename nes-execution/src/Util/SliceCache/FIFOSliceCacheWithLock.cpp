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
#include <Util/SliceCache/FIFOSliceCacheWithLock.hpp>

namespace NES::Runtime::Execution::Operators
{

FIFOSliceCacheWithLock::FIFOSliceCacheWithLock(uint64_t cacheSize, SliceAssigner sliceAssigner)
    : cacheSize(cacheSize), sliceAssigner(sliceAssigner)
{
}

FIFOSliceCacheWithLock::FIFOSliceCacheWithLock(FIFOSliceCacheWithLock const& other)
    : cacheSize(other.cacheSize), sliceAssigner(other.sliceAssigner)
{
}

FIFOSliceCacheWithLock::~FIFOSliceCacheWithLock()
{
}

SliceCachePtr FIFOSliceCacheWithLock::clone() const
{
    return std::make_shared<FIFOSliceCacheWithLock>(*this);
}

std::optional<SliceCache::SlicePtr> FIFOSliceCacheWithLock::getSliceFromCache(Timestamp timestamp)
{
    // Get slice end as a unique sliceId
    auto sliceId = sliceAssigner.getSliceEndTs(timestamp).getRawValue();

    // Get read lock for cache
    auto cacheLocked = cache.rlock();

    // Search for corresponding slice
    auto it = cacheLocked->find(sliceId);

    if (it == cacheLocked->end())
    {
        // If slice is not found, return nullopt
        return {};
    }

    // Return pointer to the slice
    return it->second;
}

bool FIFOSliceCacheWithLock::passSliceToCache(Timestamp sliceId, SliceCache::SlicePtr newSlice)
{
    // Get the value of sliceId
    auto sliceIdValue = sliceId.getRawValue();

    {
        // Get read lock for cache
        auto cacheLocked = cache.rlock();

        // Check if slice is already in cache
        if (cacheLocked->contains(sliceIdValue))
        {
            return false;
        }
    }

    // Get write lock for cache and slices
    auto [cacheWLocked, slicesLocked] = folly::acquireLocked(cache, slices);

    // Check if cache is full
    if (cacheWLocked->size() == cacheSize)
    {
        // If full, remove last slice in queue
        cacheWLocked->erase(slicesLocked->back());
        slicesLocked->pop_back();
    }
    // Add new slice to cache
    slicesLocked->push_front(sliceIdValue);
    cacheWLocked->emplace(sliceIdValue, newSlice);

    return true;
}

void FIFOSliceCacheWithLock::deleteSlicesFromCache(std::vector<Timestamp> slicesToDelete)
{
    // Get write lock for cache and slices
    auto [cacheLocked, slicesLocked] = folly::acquireLocked(cache, slices);

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
        cacheLocked->erase(sliceId);
        slicesLocked->remove(sliceId);
    }
}

} // namespace NES::Runtime::Execution::Operators