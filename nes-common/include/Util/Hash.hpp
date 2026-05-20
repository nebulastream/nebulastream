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

#include <array>
#include <cstddef>
#include <functional>
#include <unordered_map>
#include <variant>
#include <vector>
#include <folly/hash/Hash.h>

namespace NES
{

/// Transparent hasher with overloads for the standard containers that lack a
/// default std::hash specialization (std::vector, std::array, std::unordered_map).
/// Falls back to std::hash<T> for any other type, so it can be used as a drop-in
/// hasher for folly::hash::hash_combine_generic / std::unordered_*.
///
/// Defining these overloads as a program-defined functor avoids the ODR/UB
/// hazard of specializing std::hash for std-owned types (cert-dcl58-cpp).
struct Hash
{
    template <typename T>
    [[nodiscard]] size_t operator()(const std::vector<T>& vector) const noexcept
    {
        return folly::hash::hash_range(vector.begin(), vector.end());
    }

    template <typename T, size_t N>
    [[nodiscard]] size_t operator()(const std::array<T, N>& array) const noexcept
    {
        return folly::hash::hash_range(array.begin(), array.end());
    }

    template <typename K, typename V, typename Hash, typename KeyEqual>
    [[nodiscard]] size_t operator()(const std::unordered_map<K, V, Hash, KeyEqual>& map) const noexcept
    {
        return folly::hash::commutative_hash_combine_range(map.begin(), map.end());
    }

    /// Visits the active alternative and recurses into NES::Hash itself, so a variant whose
    /// alternatives are vector/array/etc. hashes correctly even though std::hash<variant> would
    /// be poisoned in that case.
    template <typename... Ts>
    [[nodiscard]] size_t operator()(const std::variant<Ts...>& variant) const noexcept
    {
        const auto altHash = std::visit([this](const auto& alt) -> size_t { return (*this)(alt); }, variant);
        return folly::hash::hash_combine_generic(*this, variant.index(), altHash);
    }

    template <typename T>
    [[nodiscard]] size_t operator()(const T& value) const noexcept
    {
        return std::hash<T>{}(value);
    }
};

}
