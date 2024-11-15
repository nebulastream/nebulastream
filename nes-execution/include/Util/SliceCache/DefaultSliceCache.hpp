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

namespace NES::Runtime::Execution::Operators {

/**
 * @brief The default slice cache does not store any slices.
 */
class DefaultSliceCache : public SliceCache {
  public:
    /**
     * @brief constructor
     */
    explicit DefaultSliceCache();

    /**
     * @brief destructor
     */
    ~DefaultSliceCache() override;

    std::optional<SlicePtr> getSliceFromCache(uint64_t sliceId) override;
    bool passSliceToCache(uint64_t sliceId, SlicePtr slice) override;
};

}// namespace NES::Runtime::Execution::Operators

