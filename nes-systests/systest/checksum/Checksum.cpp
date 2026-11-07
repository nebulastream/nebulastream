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

#include <Checksum.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <numeric>
#include <ostream>
#include <string_view>

void Checksum::add(std::string_view data)
{
    const auto localNumberOfTuples = std::ranges::count(data, '\n');
    const auto localChecksum = std::accumulate(data.cbegin(), data.cend(), static_cast<size_t>(0));

    checksum.fetch_add(localChecksum, std::memory_order::relaxed);
    numberOfTuples.fetch_add(localNumberOfTuples, std::memory_order::relaxed);
}

void Checksum::addChangelog(std::string_view data, ChangelogPosition& position)
{
    /// Unsigned arithmetic wraps, so adding the two's complement subtracts, independent of the order in which rows arrive.
    size_t localChecksum = 0;
    size_t localNumberOfTuples = 0;
    for (const char character : data)
    {
        if (position.atLineStart)
        {
            /// -U and -D withdraw a row, +I and +U add one.
            position.atLineStart = false;
            position.inKind = true;
            position.sign = character == '-' ? -1 : 1;
        }
        if (position.inKind)
        {
            position.inKind = character != ',';
            continue;
        }
        localChecksum += static_cast<size_t>(position.sign) * static_cast<size_t>(character);
        if (character == '\n')
        {
            localNumberOfTuples += static_cast<size_t>(position.sign);
            position.atLineStart = true;
        }
    }
    checksum.fetch_add(localChecksum, std::memory_order::relaxed);
    numberOfTuples.fetch_add(localNumberOfTuples, std::memory_order::relaxed);
}

std::ostream& operator<<(std::ostream& os, const Checksum& obj)
{
    return os << "checksum: " << obj.checksum << " numberOfTuples: " << obj.numberOfTuples;
}
