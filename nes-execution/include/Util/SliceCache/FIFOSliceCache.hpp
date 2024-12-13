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
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Util/SliceCache/SliceCache.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This slice cache stores the slices in a FIFO queue.
 */
class FIFOSliceCache : public SliceCache
{
    using listPosition = std::list<uint64_t>::iterator;

public:
    /**
     * @brief constructor
     * @param cacheSize
     */
    explicit FIFOSliceCache(uint64_t cacheSize, SliceAssigner sliceAssigner);

    /**
     * @brief destructor
     */
    ~FIFOSliceCache() override;

    /**
     * @brief Retrieves the slice that corresponds to the sliceId from the cache.
     * @param sliceId
     * @return SlicePtr
     */
    std::optional<SlicePtr> getSliceFromCache(Timestamp timestamp) override;

    /**
     * @brief Adds a new slice to the front of the cache, if it is not in the cache yet.
     * Returns true if the slice was successfully inserted. 
     * @param newSlice
     * @return bool
     */
    bool passSliceToCache(Timestamp timestamp, SlicePtr newSlice) override;

    /**
     * @brief Deletes a slice from the cache.
     * @param sliceId
     */
    void deleteSliceFromCache(Timestamp timestamp) override;

private:
    uint64_t cacheSize;
    SliceAssigner sliceAssigner;
    std::list<Timestamp::Underlying> slices;
    std::map<Timestamp::Underlying, SlicePtr> cache;
};

} // namespace NES::Runtime::Execution::Operators
