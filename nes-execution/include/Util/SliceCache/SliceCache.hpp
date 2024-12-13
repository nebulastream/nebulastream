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

#include <memory>
#include <optional>
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
     * @brief Retrieves the slice that corresponds to the sliceId from the cache.
     * @param sliceId
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
    virtual bool passSliceToCache(Timestamp timestamp, SlicePtr slice) = 0;

    /**
     * @brief Deletes a slice from the cache.
     * @param sliceId
     */
    virtual void deleteSliceFromCache(Timestamp timestamp) = 0;
};

} // namespace NES::Runtime::Execution::Operators
