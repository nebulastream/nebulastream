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
#include <SliceCache/SliceCache.hpp>

#include <cstdint>
#include <memory>
#include <SliceCache/SliceCacheNone.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{
SliceCache::SliceCache(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : startOfAllEntries(nullptr), numberOfEntries(numberOfEntries), sizeOfEntry(sizeOfEntry)
{
}

SliceCache::SliceCache(const SliceCache& cache)
    : startOfAllEntries(nullptr), numberOfEntries(cache.numberOfEntries), sizeOfEntry(cache.sizeOfEntry)
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

void SliceCache::setStartOfEntries(SliceCacheEntry* startOfEntries)
{
    this->startOfAllEntries = startOfEntries;
}

uint64_t SliceCache::getCacheMemorySize() const
{
    return numberOfWorkerThreads * numberOfEntries * sizeOfEntry;
}

void SliceCache::setNumberOfWorkerThreads(const uint64_t numberOfWorkerThreads)
{
    this->numberOfWorkerThreads = numberOfWorkerThreads;
}

}
