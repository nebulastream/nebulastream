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

#include <atomic>
#include <concepts>
#include <cstdint>
#include <numeric>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{
/// Calculates and stores a rolling average over the last n items
/// IMPORTANT: This class is NOT thread-safe, as there is no synchronization between a call to add() and getAverage().
template <typename T>
requires(std::integral<T> || std::floating_point<T>)
class RollingAverage
{
    std::vector<T> buffer;
    uint64_t windowSize;
    size_t index{0};
    size_t rollingCount{0};
    T average;

    void updateAverage()
    {
        if (rollingCount == 0)
        {
            average = 0;
        }

        const double sum = std::accumulate(buffer.begin(), buffer.end(), 0.0);
        average = sum / rollingCount;
    }

public:
    explicit RollingAverage(size_t windowSize) : buffer(windowSize, 0), windowSize(windowSize)
    {
        PRECONDITION(windowSize > 0, "Window size {} must be greater than 0", windowSize);
    }

    RollingAverage(const RollingAverage& rhs) = default;
    RollingAverage(RollingAverage&& rhs) noexcept = default;
    RollingAverage& operator=(const RollingAverage& rhs) = default;
    RollingAverage& operator=(RollingAverage&& rhs) noexcept = default;

    double add(T val)
    {
        if (rollingCount < windowSize)
        {
            ++rollingCount;
        }

        buffer[index] = val;
        index = (index + 1) % windowSize;

        updateAverage();
        return getAverage();
    }

    [[nodiscard]] T getAverage() const { return average; }
};

}
