/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODES_UTIL_ITERATORS_DEPTHFIRSTNODEITERATOR_HPP_
#define NES_INCLUDE_NODES_UTIL_ITERATORS_DEPTHFIRSTNODEITERATOR_HPP_
#include <memory>
#include <stack>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

/**
 * @brief Depth-First iterator for node trees.
 * We first iterate over all children and then process nodes at the same level.
 */
class DepthFirstNodeIterator {
  public:
    explicit DepthFirstNodeIterator(NodePtr start);
    DepthFirstNodeIterator() = default;

    class iterator : public std::iterator<std::forward_iterator_tag, NodePtr, NodePtr, NodePtr*, NodePtr&> {
        friend class DepthFirstNodeIterator;

      public:
        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position.
         */
        bool operator!=(const iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return
         */
        NodePtr operator*();

      private:
        explicit iterator(NodePtr current);
        explicit iterator();
        std::stack<NodePtr> workStack;
    };

    /**
     * @brief Starts a new iterator at the start node.
     * @return iterator.
     */
    iterator begin();
    /**
     * @brief The end of this iterator has an empty work stack.
     * @return iterator.
     */
    iterator end();

  private:
    NodePtr start;
};
}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_ITERATORS_DEPTHFIRSTNODEITERATOR_HPP_
