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
#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_SLIDINGWINDOWASSIGNER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_SLIDINGWINDOWASSIGNER_HPP_

#include <algorithm>
#include <cinttypes>

namespace NES::Windowing::Experimental {

/**
 * @brief The SlidingWindowAssigner assigner determines the start and end time stamp of a slice for
 * a specific window definition, that consists of a window size and a window slide.
 * @note Tumbling windows are in general modeled at this point as sliding windows with the size is equals to the slide.
 */
class SlidingWindowAssigner {
  public:
    explicit SlidingWindowAssigner(uint64_t windowSize, uint64_t windowSlide);

    /**
     * @brief Calculates the start of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the start of the particular slice.
     * @return uint64_t slice start
     */
    inline uint64_t getSliceStartTs(uint64_t ts) const {
        auto prevSlideStart = ts - ((ts) % windowSlide);
        auto prevWindowStart = ts < windowSize ? prevSlideStart : ts - ((ts - windowSize) % windowSlide);
        return std::max(prevSlideStart, prevWindowStart);
    }

    /**
     * @brief Calculates the end of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the end of the particular slice.
     * @return uint64_t slice end
     */
    inline uint64_t getSliceEndTs(uint64_t ts) const {
        auto nextSlideEnd = ts + windowSlide - ((ts) % windowSlide);
        auto nextWindowEnd = ts < windowSize ? windowSize : ts + windowSlide - ((ts - windowSize) % windowSlide);
        return std::min(nextSlideEnd, nextWindowEnd);
    }

  private:
    const uint64_t windowSize;
    const uint64_t windowSlide;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_SLIDINGWINDOWASSIGNER_HPP_
