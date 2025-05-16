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
#include <cstddef>
#include <iterator>
#include <queue>
#include <ranges>
#include <ErrorHandling.hpp>

namespace NES
{

/// Requires a function getChildren() and ==operator
template <typename T>
concept HasChildren = requires(T t) {
    { t.getChildren() } -> std::ranges::range;
} && std::equality_comparable<T>;

/// Defines a Breadth-first iterator on classes defining `getChildren()`
/// Example usage:
/// for (auto i : BFSRange(ClassWithChildren))
template <HasChildren T>
class BFSIterator
{
public:
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::input_iterator_tag;
    using iterator_concept = std::input_iterator_tag;
    using pointer = T*;
    using reference = T&;

    BFSIterator& operator++()
    {
        if (!nodeQueue.empty())
        {
            auto current = nodeQueue.front();
            nodeQueue.pop();

            for (const auto& child : current.getChildren())
            {
                nodeQueue.push(child);
            }
        }
        return *this;
    }

    void operator++(int) { ++(*this); }

    bool operator==(std::default_sentinel_t) const noexcept { return nodeQueue.empty(); }

    friend bool operator==(std::default_sentinel_t sentinel, const BFSIterator& iterator) noexcept { return iterator == sentinel; }

    [[nodiscard]] value_type operator*() const
    {
        INVARIANT(!nodeQueue.empty(), "Attempted to dereference end iterator");
        return nodeQueue.front();
    }

    friend bool operator==(const BFSIterator& lhs, const BFSIterator& rhs) noexcept
    {
        if (lhs.nodeQueue.empty() and rhs.nodeQueue.empty())
        {
            return true;
        }
        if (lhs.nodeQueue.empty() or rhs.nodeQueue.empty())
        {
            return false;
        }
        return lhs.nodeQueue.front() == rhs.nodeQueue.front();
    }

    friend bool operator!=(const BFSIterator& lhs, const BFSIterator& rhs) noexcept { return !(lhs == rhs); }

private:
    template <typename>
    friend class BFSRange;
    BFSIterator() = default;
    explicit BFSIterator(T root) { nodeQueue.push(root); }

    std::queue<T> nodeQueue;
};

template <typename T>
class BFSRange : public std::ranges::view_interface<BFSRange<T>>
{
public:
    explicit BFSRange(T root) : root(root) { }

    BFSIterator<T> begin() const { return BFSIterator<T>(root); }
    [[nodiscard]] std::default_sentinel_t end() const noexcept { return {}; }

private:
    T root;
};
}
