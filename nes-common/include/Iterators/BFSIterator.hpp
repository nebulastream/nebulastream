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
#include <iterator>
#include <memory>
#include <queue>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Requires a function getChildren()
template <typename T>
concept HasChildren = requires(T t) {
    { t.getChildren() } -> std::ranges::range;
};

/// Defines a Breadth-first iterator on classes defining `getChildren()`
/// Example usage:
/// for (auto i : BFSRange(ClassWithChildren))
template <HasChildren T>
class BFSIterator
{
public:
    using value_type        = T;
    using difference_type   = std::ptrdiff_t;
    using iterator_category = std::input_iterator_tag;
    using iterator_concept  = std::input_iterator_tag;

    BFSIterator() = default;
    explicit BFSIterator(T root) { nodeQueue.push(root); }

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

    bool operator==(std::default_sentinel_t) const noexcept
    {
        return nodeQueue.empty();
    }
    friend bool operator==(std::default_sentinel_t s, const BFSIterator& it) noexcept
    {
        return it == s;
    }

    [[nodiscard]] value_type operator*() const
    {
        INVARIANT(!nodeQueue.empty(), "Attempted to dereference end iterator");
        return nodeQueue.front();
    }

private:
    std::queue<T> nodeQueue;
};
}

template <typename T>
class BFSRange  : public std::ranges::view_interface<BFSRange<T>>
{
public:
    explicit BFSRange(T root) : root(root) { }

    BFSIterator<T> begin() const { return BFSIterator<T>(root); }
    std::default_sentinel_t end()  const noexcept { return {}; }

private:
    T root;
};
}
