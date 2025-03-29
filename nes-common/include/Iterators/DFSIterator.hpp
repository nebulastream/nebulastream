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

#include <memory>
#include <stack>
#include <vector>

namespace NES
{

// Requires that T contains a function getChildren()
template <typename T>
class DFSIterator
{
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::shared_ptr<T>;
    using difference_type = std::ptrdiff_t;
    using pointer = std::shared_ptr<T>*;
    using reference = std::shared_ptr<T>&;

    DFSIterator() = default;

    explicit DFSIterator(std::shared_ptr<T> root)
    {
        if (root)
        {
            nodeStack.push(std::move(root));
        }
    }

    DFSIterator& operator++()
    {
        if (!nodeStack.empty())
        {
            auto current = nodeStack.top();
            nodeStack.pop();

            auto children = current->children;
            for (auto it = children.rbegin(); it != children.rend(); ++it)
            {
                nodeStack.push(*it);
            }
        }
        return *this;
    }

    bool operator==(const DFSIterator& other) const { return nodeStack.empty() && other.nodeStack.empty(); }
    bool operator!=(const DFSIterator& other) const { return !(*this == other); }

    std::shared_ptr<T> operator*() const
    {
        if (!nodeStack.empty())
        {
            return nodeStack.top();
        }
        return nullptr;
    }

private:
    std::stack<std::shared_ptr<T>> nodeStack;
};

template <typename T>
class DFSRange
{
public:
    explicit DFSRange(std::shared_ptr<T> root) : root(std::move(root)) { }

    DFSIterator<T> begin() const { return DFSIterator<T>(root); }

    DFSIterator<T> end() const { return DFSIterator<T>(); }

private:
    std::shared_ptr<T> root;
};

}
