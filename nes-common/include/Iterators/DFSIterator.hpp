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
#include <stack>
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
/// for (auto i : DFSRange(ClassWithChildren))
template <HasChildren T>
class DFSIterator
{
public:
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::input_iterator_tag;
    using iterator_concept = std::input_iterator_tag;
    using pointer = T*;
    using reference = T&;

    DFSIterator& operator++()
    {
        if (!nodeStack.empty())
        {
            auto current = nodeStack.top();
            nodeStack.pop();

            for (const auto& child : current.getChildren())
            {
                nodeStack.push(child);
            }
        }
        return *this;
    }

    void operator++(int) { ++(*this); }

    bool operator==(std::default_sentinel_t) const noexcept { return nodeStack.empty(); }

    friend bool operator==(std::default_sentinel_t sentinel, const DFSIterator& iterator) noexcept { return iterator == sentinel; }

    [[nodiscard]] value_type operator*() const
    {
        INVARIANT(!nodeStack.empty(), "Attempted to dereference end iterator");
        return nodeStack.top();
    }

    friend bool operator==(const DFSIterator& lhs, const DFSIterator& rhs) noexcept
    {
        if (lhs.nodeStack.empty() and rhs.nodeStack.empty())
        {
            return true;
        }
        if (lhs.nodeStack.empty() or rhs.nodeStack.empty())
        {
            return false;
        }
        return lhs.nodeStack.top() == rhs.nodeStack.top();
    }

    friend bool operator!=(const DFSIterator& lhs, const DFSIterator& rhs) noexcept { return !(lhs == rhs); }

private:
    template <typename>
    friend class DFSRange;
    DFSIterator() = default;
    explicit DFSIterator(T root) { nodeStack.push(root); }

    std::stack<T> nodeStack;
};

template <typename T>
class DFSRange : public std::ranges::view_interface<DFSRange<T>>
{
public:
    explicit DFSRange(T root) : root(root) { }

    DFSIterator<T> begin() const { return DFSIterator<T>(root); }
    [[nodiscard]] std::default_sentinel_t end() const noexcept { return {}; }

private:
    T root;
};
}
