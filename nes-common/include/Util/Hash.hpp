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

#include <ranges>
#include <folly/hash/Hash.h>
#include <unordered_map>

template <typename T>
struct std::hash<std::vector<T>>
{
    size_t operator()(const std::vector<T>& vector) const noexcept { return folly::hash::hash_range(vector.begin(), vector.end()); }
};

template <typename T, size_t N>
struct std::hash<std::array<T, N>>
{
    size_t operator()(const std::array<T, N>& array) const noexcept { return folly::hash::hash_range(array.begin(), array.end()); }
};

// template <typename T, typename R>
// struct std::hash<std::pair<T, R>> {
//     si
// };

template <typename T, typename R>
struct std::hash<std::unordered_map<T, R>>
{
    size_t operator()(const std::unordered_map<T, R>& map) const noexcept {
        return folly::hash::commutative_hash_combine_range(map.begin(), map.end());
        // map | std::ranges::to<std::vector>();
    }
};