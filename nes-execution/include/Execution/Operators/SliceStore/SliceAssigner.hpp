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

#include <algorithm>
#include <Identifiers/Identifiers.hpp>

#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief The SliceAssigner assigner determines the start and end time stamp of a slice for
 * a specific window definition, that consists of a window size and a window slide.
 * @note Tumbling windows are in general modeled at this point as sliding windows with the size is equals to the slide.
 */
class SliceAssigner
{
public:
    explicit SliceAssigner(const uint64_t windowSize, const uint64_t windowSlide) : windowSize(windowSize), windowSlide(windowSlide)
    {
        INVARIANT(
            windowSize >= windowSlide, "Currently the window assigner does not support windows with a larger slide then the window size.");
    }

    /**
     * @brief Calculates the start of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the start of the particular slice.
     * @return uint64_t slice start
     */
    [[nodiscard]] SliceStart getSliceStartTs(const Timestamp ts) const
    {
        const auto timestampRaw = ts.getRawValue();
        auto prevSlideStart = timestampRaw - ((timestampRaw) % windowSlide);
        auto prevWindowStart = timestampRaw < windowSize ? prevSlideStart : timestampRaw - ((timestampRaw - windowSize) % windowSlide);
        return SliceStart(std::max(prevSlideStart, prevWindowStart));
    }

    /**
     * @brief Calculates the end of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the end of the particular slice.
     * @return uint64_t slice end
     */
    [[nodiscard]] SliceEnd getSliceEndTs(const Timestamp ts) const
    {
        const auto timestampRaw = ts.getRawValue();
        auto nextSlideEnd = timestampRaw + windowSlide - ((timestampRaw) % windowSlide);
        auto nextWindowEnd
            = timestampRaw < windowSize ? windowSize : timestampRaw + windowSlide - ((timestampRaw - windowSize) % windowSlide);
        return SliceEnd(std::min(nextSlideEnd, nextWindowEnd));
    }

    /**
     * @brief Getter for the window size
     * @return window size in uint64_t
     */
    uint64_t getWindowSize() const { return windowSize; }

    /**
     * @brief Getter for the window slide
     * @return window slide in uint64_t
     */
    uint64_t getWindowSlide() const { return windowSlide; }

private:
    const uint64_t windowSize;
    const uint64_t windowSlide;
};

}
