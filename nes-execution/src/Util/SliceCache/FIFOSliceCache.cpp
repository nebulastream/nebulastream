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

namespace NES::Runtime::Execution::Operators
{

FIFOSliceCache::FIFOSliceCache(uint64_t cacheSize, SliceAssigner sliceAssigner) : cacheSize(cacheSize), sliceAssigner(sliceAssigner)
{
}

FIFOSliceCache::FIFOSliceCache(FIFOSliceCache const& other) : cacheSize(other.cacheSize), sliceAssigner(other.sliceAssigner)
{
}

FIFOSliceCache::~FIFOSliceCache()
{
}

SliceCachePtr FIFOSliceCache::clone() const
{
    return std::make_shared<FIFOSliceCache>(*this);
}

std::optional<SliceCache::SlicePtr> FIFOSliceCache::getSliceFromCache(Timestamp timestamp)
{
    // Get slice end as a unique sliceId
    auto sliceId = sliceAssigner.getSliceEndTs(timestamp).getRawValue();

    // Search for corresponding slice
    auto it = cache.find(sliceId);

    if (it == cache.end())
    {
        // If slice is not found, return nullopt
        return {};
    }

    // Return pointer to the slice
    return it->second;
}

bool FIFOSliceCache::passSliceToCache(Timestamp sliceId, SliceCache::SlicePtr newSlice)
{
    // Get the value of sliceId
    auto sliceIdValue = sliceId.getRawValue();

    // Check if slice is already in cache
    if (cache.contains(sliceIdValue))
    {
        return false;
    }

    // Check if cache is full
    if (cache.size() == cacheSize)
    {
        // If full, remove last slice in queue
        cache.erase(slices.back());
        slices.pop_back();
    }
    // Add new slice to cache
    slices.push_front(sliceIdValue);
    cache.emplace(sliceIdValue, newSlice);

    return true;
}

void FIFOSliceCache::deleteSlicesFromCache(std::vector<Timestamp>)
{
}

} // namespace NES::Runtime::Execution::Operators