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

#include "Checksum.hpp"

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

std::ostream& operator<<(std::ostream& os, const Checksum& obj)
{
    return os << "checksum: " << obj.checksum << " numberOfTuples: " << obj.numberOfTuples;
}
