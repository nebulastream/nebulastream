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

namespace NES
{
/// Calculates and stores a rolling average over the last n items
/// IMPORTANT: This class is NOT thread-safe
template <typename T>
class RollingAverage
{
    std::vector<T> buffer;
    uint64_t n;
    uint64_t index;
    T sum;
    uint64_t count;

public:
    explicit RollingAverage(uint64_t size) : buffer(size, 0), n(size), index(0), sum(0), count(0) { }

    T add(T val)
    {
        if (count < n)
        {
            sum += val;
            buffer[index] = val;
            ++count;
        }
        else
        {
            sum += val - buffer[index];
            buffer[index] = val;
        }
        index = (index + 1) % n;

        return getAverage();
    }

    T getAverage() { return count == 0 ? T(0) : sum / count; }
};

}
