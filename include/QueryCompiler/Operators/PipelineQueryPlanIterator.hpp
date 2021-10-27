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

#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_PIPELINE_QUERY_PLAN_ITERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_PIPELINE_QUERY_PLAN_ITERATOR_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <iterator>
#include <stack>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Iterator for pipeline query plans, which correctly handles multiple sources and sinks.
 * The iterator visits each pipeline exactly one time in the following order:
 * top-to-bottom and left-to-right
 * @see
 */
class PipelineQueryPlanIterator {
  public:
    explicit PipelineQueryPlanIterator(PipelineQueryPlanPtr queryPlan);

    class iterator : public std::iterator<std::forward_iterator_tag,
                                          OperatorPipelinePtr,
                                          OperatorPipelinePtr,
                                          OperatorPipelinePtr*,
                                          OperatorPipelinePtr&> {
        // use PipelineQueryPlanIterator as a fiend to access its state
        friend class PipelineQueryPlanIterator;

      public:
        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         * @return boolean
         */
        bool operator!=(const iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return OperatorPipelinePtr
         */
        OperatorPipelinePtr operator*();

      private:
        explicit iterator(const PipelineQueryPlanPtr& current);
        explicit iterator();
        std::stack<OperatorPipelinePtr> workStack;
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
    static iterator end();

    /**
     * @brief Return a snapshot of the iterator.
     * @return vector<NodePtr> nodes
     */
    std::vector<OperatorPipelinePtr> snapshot();

  private:
    PipelineQueryPlanPtr queryPlan;
};
}// namespace QueryCompilation

}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_OPERATORS_PIPELINE_QUERY_PLAN_ITERATOR_HPP_
