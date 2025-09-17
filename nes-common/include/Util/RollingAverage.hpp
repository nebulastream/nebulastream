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

#include <concepts>
#include <cstdint>
#include <numeric>
#include <vector>

namespace NES
{
/// Calculates and stores a rolling average over the last n items
/// IMPORTANT: This class is NOT thread-safe
template <typename T>
requires(std::integral<T> || std::floating_point<T>)
class RollingAverage
{
    std::vector<T> buffer;
    uint64_t windowSize;
    uint64_t index{0};
    uint64_t rollingCount{0};

public:
    explicit RollingAverage(uint64_t windowSize) : buffer(windowSize, 0), windowSize(windowSize) { }

    double add(T val)
    {
        if (rollingCount < windowSize)
        {
            ++rollingCount;
        }

        buffer[index] = val;
        index = (index + 1) % windowSize;

        return getAverage();
    }

    double getAverage() const
    {
        if (rollingCount == 0)
        {
            return T(0);
        }

        const double sum = std::accumulate(buffer.begin(), buffer.end(), 0.0);
        return sum / rollingCount;
    }
};

}
