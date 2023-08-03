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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICESTORE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICESTORE_HPP_

#include <list>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class NonKeyedSlice;
using GlobalSlicePtr = std::unique_ptr<NonKeyedSlice>;

/**
* @brief The slice store for sliding windows that maintains a set of slices for all in-flight sliding windows.
*/
template<class SliceType>
class SlidingWindowSliceStore {
  public:
    using SliceTypePtr = std::shared_ptr<SliceType>;
    explicit SlidingWindowSliceStore(uint64_t windowSize, uint64_t windowSlide);
    virtual ~SlidingWindowSliceStore() = default;

    /**
     * @brief Inserts a new slice to the slice store.
     */
    void insertSlice(SliceTypePtr slice);

    /**
     * @brief Returns the slice end ts for the first slice in the slice store.
     * @return uint64_t
     */
    SliceTypePtr& getFirstSlice();

    /**
     * @brief Returns the slice end ts for the last slice in the slice store.
     * @return uint64_t
     */
    SliceTypePtr& getLastSlice();

    /**
     * @brief Collect a set of windows between oldTs and currentTs.
     * @param oldTs
     * @param currentTs
     * @return set of windows
     */
    std::set<std::tuple<uint64_t, uint64_t>> collectWindows(uint64_t oldTs, uint64_t currentTs);

    /**
     * @brief Collect all windows that are covered by any slices in the slice store
     * @param oldTs
     * @return set of windows
     */
    std::set<std::tuple<uint64_t, uint64_t>> collectAllWindows(uint64_t oldTs);

    /**
     * @brief Collect slices  that are part of a window
     * @param windowStart
     * @param windowEnd
     * @return  td::vector<SliceTypePtr>
     */
    std::vector<SliceTypePtr> collectSlicesForWindow(uint64_t windowStart, uint64_t windowEnd);

    /**
     * @brief Remove all slices from the slice store that are not required for any windows anymore
     * @param ts
     */
    void removeSlices(uint64_t ts);

    /**
     * @brief Returns the number of currently stored slices
     * @return uint64_t
     */
    uint64_t getNumberOfSlices();

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::mutex sliceStagingMutex;
    std::list<SliceTypePtr> slices;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICESTORE_HPP_