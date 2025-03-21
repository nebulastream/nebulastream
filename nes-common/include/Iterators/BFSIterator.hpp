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

namespace NES
{

// Requires that T contains a function getChildren()
template <typename T>
class BFSIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using reference = T&;

    BFSIterator() = default;
    explicit BFSIterator(T root) {
        nodeQueue.push(root);
    }

    BFSIterator& operator++() {
        if (!nodeQueue.empty()) {
            auto current = nodeQueue.front();
            nodeQueue.pop();

            for (const auto& child : current.getChildren()) {
                // child is a const std::unique_ptr<T>&, so take its raw pointer.
                nodeQueue.push(child);
            }
        }
        return *this;
    }

    bool operator==(const BFSIterator& other) const {
        return nodeQueue.empty() && other.nodeQueue.empty();
    }
    bool operator!=(const BFSIterator& other) const { return !(*this == other); }

    T operator*() const {
        if (!nodeQueue.empty()) {
            return nodeQueue.front();
        }
        return nullptr;
    }

private:
    std::queue<T> nodeQueue;
};

template <typename T>
class BFSRange {
public:
    explicit BFSRange(T root) : root(root) { }

    BFSIterator<T> begin() const { return BFSIterator<T>(root); }
    BFSIterator<T> end() const { return BFSIterator<T>(); }

private:
    T root;
};
}
