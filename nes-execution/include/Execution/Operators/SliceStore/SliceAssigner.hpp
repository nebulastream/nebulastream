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
#include <cstdint>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
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
    explicit SliceAssigner(const Timestamp::Underlying windowSize, const Timestamp::Underlying windowSlide)
        : windowSize(windowSize), windowSlide(windowSlide)
    {
        INVARIANT(
            windowSize >= windowSlide, "Currently the window assigner does not support windows with a larger slide then the window size.");
    }

    SliceAssigner(const SliceAssigner& other) = default;
    SliceAssigner(SliceAssigner&& other) noexcept = default;
    SliceAssigner& operator=(const SliceAssigner& other) = default;
    SliceAssigner& operator=(SliceAssigner&& other) noexcept = default;

    ~SliceAssigner() = default;

    /**
     * @brief Calculates the start of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the start of the particular slice.
     * @return Timestamp::Underlying slice start
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
     * @return Timestamp::Underlying slice end
     */
    [[nodiscard]] SliceEnd getSliceEndTs(const Timestamp ts) const
    {
        const auto timestampRaw = ts.getRawValue();
        const auto nextSlideEnd = timestampRaw + windowSlide - ((timestampRaw) % windowSlide);
        const auto nextWindowEnd
            = timestampRaw < windowSize ? windowSize : timestampRaw + windowSlide - ((timestampRaw - windowSize) % windowSlide);
        return SliceEnd(std::min(nextSlideEnd, nextWindowEnd));
    }

    [[nodiscard]] Timestamp::Underlying getWindowSize() const { return windowSize; }

    [[nodiscard]] Timestamp::Underlying getWindowSlide() const { return windowSlide; }

private:
    Timestamp::Underlying windowSize;
    Timestamp::Underlying windowSlide;
};

}
