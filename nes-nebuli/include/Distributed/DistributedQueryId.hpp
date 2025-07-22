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

#include <cstddef>
#include <string>
#include <vector>

namespace NES::Distributed
{
struct LocalQueryId
{
    size_t id;
    std::string grpcAddr;

    bool operator==(const LocalQueryId& other) const = default;
};

struct QueryId
{
    std::vector<LocalQueryId> localQueries;

    static Distributed::QueryId load(const std::string& identifier);
    static std::string save(const Distributed::QueryId& queryId);
    bool operator==(const Distributed::QueryId& other) const { return localQueries == other.localQueries; }
    auto view() const { return localQueries; }
    auto begin() const { return localQueries.cbegin(); }
    auto end() const { return localQueries.cend(); }
};

}

template <>
struct std::hash<NES::Distributed::QueryId>
{
    size_t operator()(const NES::Distributed::QueryId& queryId) const noexcept
    {
        size_t result = 0;

        for (const auto& [localQueryId, grpcAddr] : queryId.localQueries)
        {
            constexpr std::hash<size_t> sizeHasher;
            constexpr std::hash<std::string> stringHasher;
            result ^= sizeHasher(localQueryId) + 0x9e3779b9 + (result << 6) + (result >> 2);
            result ^= stringHasher(grpcAddr) + 0x9e3779b9 + (result << 6) + (result >> 2);
        }

        return result;
    }
};
