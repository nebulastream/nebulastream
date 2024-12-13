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
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Util/SliceCache/LeastRecentlyUsedSliceCache.hpp>

namespace NES::Runtime::Execution::Operators
{

LeastRecentlyUsedSliceCache::LeastRecentlyUsedSliceCache(uint64_t cacheSize, SliceAssigner sliceAssigner)
    : cacheSize(cacheSize), sliceAssigner(sliceAssigner)
{
}

LeastRecentlyUsedSliceCache::~LeastRecentlyUsedSliceCache()
{
}

std::optional<SliceCache::SlicePtr> LeastRecentlyUsedSliceCache::getSliceFromCache(Timestamp timestamp)
{
    auto sliceId = sliceAssigner.getSliceEndTs(timestamp).getRawValue();

    // Search for corresponding slice
    auto it = cache.find(sliceId);
    if (it != cache.end())
    {
        // If found, bring slice to front of cache and return
        auto& currentTuple = it->second;
        const auto oldListPosition = std::get<listPosition>(currentTuple);

        lruSlices.erase(oldListPosition);

        lruSlices.push_front(sliceId);
        std::get<listPosition>(currentTuple) = lruSlices.begin();

        return std::get<SliceCache::SlicePtr>(currentTuple);
    }
    // If not found, return nullopt
    return {};
}

bool LeastRecentlyUsedSliceCache::passSliceToCache(Timestamp timestamp, SliceCache::SlicePtr newSlice)
{
    auto sliceId = sliceAssigner.getSliceEndTs(timestamp).getRawValue();

    // Check if slice already in cache
    if (cache.contains(sliceId))
    {
        return false;
    }
    // Check if cache full
    if (cache.size() == cacheSize)
    {
        // If full, remove least recently used slice
        cache.erase(lruSlices.back());
        lruSlices.pop_back();
    }
    // Add new slice to cache
    lruSlices.push_front(sliceId);
    cache.emplace(sliceId, std::make_tuple(lruSlices.begin(), newSlice));
    return true;
}

void LeastRecentlyUsedSliceCache::deleteSliceFromCache(Timestamp)
{
}

} // namespace NES::Runtime::Execution::Operators