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

#ifndef NES_TOPOLOGYITERATOR_HPP
#define NES_TOPOLOGYITERATOR_HPP

#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <iterator>
#include <stack>

namespace NES {
/**
 * @brief Iterator for topology.
 * The iterator visits each topology node exactly one time in the following order:
 * left-to-right and top-to-bottom
 *
 * Example Topology:
 *
 *                                       --- SrcNode1 ---
 *                                     /
 *    --- Root --- Node1 --- Node2 ---
 *                                     \
 *                                       --- Node3 --- SrcNode2
 *
 * Iteration order:
 * #1 - Root
 * #2 - Node1
 * #3 - Node2
 * #4 - SrcNode1
 * #5 - Node3
 * #6 - SrcNode2
 */
class TopologyIterator {
  public:
    explicit TopologyIterator(TopologyPtr topology);

    class iterator : public std::iterator<std::forward_iterator_tag, NodePtr, NodePtr, NodePtr*, NodePtr&> {
        friend class TopologyIterator;

      public:
        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         */
        bool operator!=(const iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return
         */
        NodePtr operator*();

      private:
        explicit iterator(TopologyPtr current);
        explicit iterator();
        std::stack<NodePtr> workStack;
    };

    /**
     * @brief Starts a new iterator at the start node, which is always a sink.
     * @return iterator
     */
    iterator begin();

    /**
    * @brief The end of this iterator has an empty work stack.
    * @return iterator
    */
    iterator end();

    /**
     * @brief Return a snapshot of the iterator.
     * @return vector<NodePtr> nodes
     */
    std::vector<NodePtr> snapshot();

  private:
    TopologyPtr topology;
};
} // namespace NES

#endif//NES_TOPOLOGYITERATOR_HPP
