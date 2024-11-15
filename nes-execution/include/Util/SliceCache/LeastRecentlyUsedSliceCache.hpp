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
#pragma once

#include <Util/SliceCache/SliceCache.hpp>
#include <folly/Synchronized.h>
#include <list>
#include <tuple>
#include <unordered_map>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This slice cache stores the most recently used slices.
 */
class LeastRecentlyUsedSliceCache : public SliceCache {

  using listPosition = std::list<uint64_t>::iterator;

  public:
    /**
     * @brief constructor
     * @param cacheSize
     */
    explicit LeastRecentlyUsedSliceCache(uint64_t cacheSize);

    /**
     * @brief destructor
     */
    ~LeastRecentlyUsedSliceCache() override;

    /**
     * @brief Retrieves the slice that corresponds to the sliceId from the cache.
     * @param sliceId
     * @return SlicePtr
     */
    std::optional<SlicePtr> getSliceFromCache(uint64_t sliceId) override;

    /**
     * @brief Adds a new slice to the front of the cache, if it is not in the cache yet.
     * Returns true if the slice was successfully inserted. 
     * @param newSlice
     * @return bool
     */
    bool passSliceToCache(uint64_t sliceId, SlicePtr newSlice) override;

  private:
    uint64_t cacheSize;
    folly::Synchronized<std::list<uint64_t>> lruSlices;
    folly::Synchronized<std::unordered_map<uint64_t, std::tuple<listPosition, SlicePtr>>> cache;
};

}// namespace NES::Runtime::Execution::Operators
