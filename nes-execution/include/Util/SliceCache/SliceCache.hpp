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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include <Execution/Operators/SliceStore/Slice.hpp>

namespace NES::Runtime::Execution::Operators
{

class SliceCache;
using SliceCachePtr = std::shared_ptr<SliceCache>;

/**
 * @brief Interface for a slice cache.
 */
class SliceCache
{
public:
    using SlicePtr = std::shared_ptr<Slice>;

    /**
     * @brief destructor
     */
    virtual ~SliceCache() = default;

    /**
     * @brief Clones an existing slice cache.
     * @return SliceCachePtr
     */
    virtual SliceCachePtr clone() const = 0;

    /**
     * @brief Retrieves the slice that corresponds to the given timestamp from the cache.
     * @param timestamp
     * @return SlicePtr
     */
    virtual std::optional<SlicePtr> getSliceFromCache(Timestamp timestamp) = 0;

    /**
     * @brief Pass a slice to the cache. It is up to each implementation whether it gets inserted and how.
     * Returns true if the slice was inserted and false if the slice was not inserted.
     * @param sliceId
     * @param slice
     * @return bool
     */
    virtual bool passSliceToCache(Timestamp sliceId, SlicePtr slice) = 0;

    /**
     * @brief Deletes the given slices from the cache.
     * @param slicesToDelete
     */
    virtual void deleteSlicesFromCache(std::vector<Timestamp> slicesToDelete) = 0;

    [[nodiscard]] uint64_t getNumberOfCacheHits() const { return hitCounter; }

    [[nodiscard]] uint64_t getNumberOfCacheMisses() const { return missCounter; }

    void resetCounters()
    {
        hitCounter = 0;
        missCounter = 0;
    }

protected:
    uint64_t hitCounter = 0;
    uint64_t missCounter = 0;
};

} // namespace NES::Runtime::Execution::Operators
