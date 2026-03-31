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
#include <SliceStore/SliceCache/SliceCache.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <SliceStore/SliceCache/SliceCacheNone.hpp>
#include <SliceStore/SliceCache/SliceCacheSecondChance.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{
SliceCache::SliceCache(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : startOfSliceCache(nullptr), numberOfEntries(numberOfEntries), sizeOfEntry(sizeOfEntry)
{
}

SliceCache::SliceCache(const SliceCache& cache)
    : startOfSliceCache(cache.startOfSliceCache), numberOfEntries(cache.numberOfEntries), sizeOfEntry(cache.sizeOfEntry)
{
}

std::unique_ptr<SliceCache> SliceCache::createSliceCache(const SliceCacheConfiguration& sliceCacheConfiguration)
{
    if (sliceCacheConfiguration.enableSliceCache.getValue())
    {
        return std::make_unique<SliceCacheSecondChance>(
            sliceCacheConfiguration.numberOfEntries.getValue(), sizeof(SliceCacheEntrySecondChance));
    }
    return std::make_unique<SliceCacheNone>();
}

uint64_t SliceCache::getCacheMemorySize() const
{
    return numberOfWorkerThreads * numberOfEntries * sizeOfEntry;
}

void SliceCache::setNumberOfWorkerThreads(const uint64_t numberOfWorkerThreads)
{
    this->numberOfWorkerThreads = numberOfWorkerThreads;
}

void SliceCache::setStartOfEntries(const std::span<std::byte>& startOfSliceCache)
{
    ///NOLINTNEXTLINE (cppcoreguidelines-pro-type-reinterpret-cast)
    this->startOfSliceCache = reinterpret_cast<SliceCacheEntry*>(startOfSliceCache.data());
}

}
