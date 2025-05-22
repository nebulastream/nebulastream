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
#include <iterator>
#include <memory>
#include <stack>
#include <Nodes/Node.hpp>

namespace NES
{


/**
 * @brief Depth-First iterator for node trees.
 * We first iterate over all children and then process nodes at the same level.
 */
class DepthFirstNodeIterator
{
public:
    explicit DepthFirstNodeIterator(std::shared_ptr<Node> start);
    DepthFirstNodeIterator() = default;

    class Iterator : public std::iterator<
                         std::forward_iterator_tag,
                         std::shared_ptr<Node>,
                         std::shared_ptr<Node>,
                         std::shared_ptr<Node>*,
                         std::shared_ptr<Node>&>
    {
        friend class DepthFirstNodeIterator;

    public:
        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        Iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position.
         */
        bool operator!=(const Iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return
         */
        std::shared_ptr<Node> operator*();

    private:
        explicit Iterator(const std::shared_ptr<Node>& current);
        explicit Iterator();
        std::stack<std::shared_ptr<Node>> workStack;
    };

    /**
     * @brief Starts a new iterator at the start node.
     * @return iterator.
     */
    [[nodiscard]] Iterator begin() const;
    /**
     * @brief The end of this iterator has an empty work stack.
     * @return iterator.
     */
    static Iterator end();

private:
    std::shared_ptr<Node> start;
};
}
