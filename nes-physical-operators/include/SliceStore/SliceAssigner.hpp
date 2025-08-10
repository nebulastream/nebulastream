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
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// @brief The SliceAssigner assigner determines the start and end timestamp of a slice for
/// a specific window definition, that consists of a window size and a window slide.
/// @note Tumbling windows are in general modeled at this point as sliding windows with the size is equals to the slide.
class SliceAssigner
{
public:
    explicit SliceAssigner(const uint64_t windowSize, const uint64_t windowSlide) : windowSize(windowSize), windowSlide(windowSlide) { }

    SliceAssigner(const SliceAssigner& other) = default;
    SliceAssigner(SliceAssigner&& other) noexcept = default;
    SliceAssigner& operator=(const SliceAssigner& other) = default;
    SliceAssigner& operator=(SliceAssigner&& other) noexcept = default;

    ~SliceAssigner() = default;

    /// @brief Calculates the start of a slice for a specific timestamp ts.
    /// @param ts the timestamp for which we calculate the start of the particular slice.
    /// @return uint64_t slice start
    [[nodiscard]] SliceStart getSliceStartTs(const Timestamp ts) const
    {
        const auto timestampRaw = ts.getRawValue();
        const auto prevSlideStart = timestampRaw - ((timestampRaw) % windowSlide);
        const auto prevWindowStart
            = timestampRaw < windowSize ? prevSlideStart : timestampRaw - ((timestampRaw - windowSize) % windowSlide);
        return SliceStart(std::max(prevSlideStart, prevWindowStart));
    }

    /// @brief Calculates the end of a slice for a specific timestamp ts.
    /// @param ts the timestamp for which we calculate the end of the particular slice.
    /// @return uint64_t slice end
    [[nodiscard]] SliceEnd getSliceEndTs(const Timestamp ts) const
    {
        const auto timestampRaw = ts.getRawValue();
        const auto nextSlideEnd = timestampRaw + windowSlide - ((timestampRaw) % windowSlide);
        const auto nextWindowEnd
            = timestampRaw < windowSize ? windowSize : timestampRaw + windowSlide - ((timestampRaw - windowSize) % windowSlide);
        return SliceEnd(std::min(nextSlideEnd, nextWindowEnd));
    }

    /// Retrieves all window identifiers that correspond to this slice
    /// It might happen that for a particular slice no windows are getting returned.
    /// For example, size of 10 and slide of 20 would mean that there does not exist a window from [10-20]
    [[nodiscard]] std::vector<WindowInfo> getAllWindowsForSlice(const Slice& slice) const
    {
        const auto sliceStart = slice.getSliceStart().getRawValue();
        const auto sliceEnd = slice.getSliceEnd().getRawValue();

        /// Taking the max out of sliceEnd and windowSize, allows us to not create windows, such as 0-5 for slide 5 and size 100.
        /// In our window model, a window is always the size of the window size.
        auto firstWindowEnd = std::max((sliceEnd), windowSize);
        auto lastWindowEnd = sliceStart + windowSize;

        if ((firstWindowEnd - windowSize) % windowSlide != 0)
        {
            /// firstWindowEnd is no valid windowEnd for the window parameters size and slide.
            /// essentially means, it is the firstWindowStart in which the slice is not contained, and we can use that to derive the required parameters
            lastWindowEnd = firstWindowEnd + windowSize - windowSlide;
            firstWindowEnd = sliceStart + windowSlide;
        }

        std::vector<WindowInfo> allWindows;
        for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide)
        {
            allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
        }

        return allWindows;
    }

    [[nodiscard]] uint64_t getWindowSize() const { return windowSize; }

    [[nodiscard]] uint64_t getWindowSlide() const { return windowSlide; }

private:
    uint64_t windowSize;
    uint64_t windowSlide;
};

}
