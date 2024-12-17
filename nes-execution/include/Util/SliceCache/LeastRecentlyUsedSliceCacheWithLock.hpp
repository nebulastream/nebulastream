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

#include <list>
#include <map>
#include <tuple>
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Util/SliceCache/SliceCache.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This slice cache stores the most recently used slices.
 */
class LeastRecentlyUsedSliceCacheWithLock : public SliceCache
{
    using listPosition = std::list<Timestamp::Underlying>::iterator;

public:
    /**
     * @brief constructor
     * @param cacheSize
     */
    explicit LeastRecentlyUsedSliceCacheWithLock(uint64_t cacheSize, SliceAssigner sliceAssigner);

    /**
     * @brief copy constructor
     * @param other
     */
    LeastRecentlyUsedSliceCacheWithLock(LeastRecentlyUsedSliceCacheWithLock const& other);

    /**
     * @brief destructor
     */
    ~LeastRecentlyUsedSliceCacheWithLock() override;

    /**
     * @brief Clones an existing slice cache.
     * @return SliceCachePtr
     */
    SliceCachePtr clone() const override;

    /**
     * @brief Retrieves the slice that corresponds to the given timestamp from the cache.
     * @param timestamp
     * @return SlicePtr
     */
    std::optional<SlicePtr> getSliceFromCache(Timestamp timestamp) override;

    /**
     * @brief Adds a new slice to the front of the cache, if it is not in the cache yet.
     * Returns true if the slice was successfully inserted.
     * @param sliceId
     * @param newSlice
     * @return bool
     */
    bool passSliceToCache(Timestamp sliceId, SlicePtr newSlice) override;

    /**
     * @brief Deletes the given slices from the cache.
     * @param slicesToDelete
     */
    void deleteSlicesFromCache(std::vector<Timestamp> slicesToDelete) override;

private:
    uint64_t cacheSize;
    SliceAssigner sliceAssigner;
    folly::Synchronized<std::list<Timestamp::Underlying>> lruSlices;
    folly::Synchronized<std::map<Timestamp::Underlying, std::tuple<listPosition, SlicePtr>>> cache;
};

} // namespace NES::Runtime::Execution::Operators
