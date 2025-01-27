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
#include <vector>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>

namespace NES::QueryCompilation
{

/**
 * @brief iterator for pipeline query plans, which correctly handles multiple sources and sinks.
 * The iterator visits each pipeline exactly one time in the following order:
 * top-to-bottom and left-to-right
 * @see
 */
class PipelineQueryPlanIterator
{
public:
    explicit PipelineQueryPlanIterator(std::shared_ptr<PipelineQueryPlan> queryPlan);

    class Iterator : public std::iterator<
                         std::forward_iterator_tag,
                         std::shared_ptr<OperatorPipeline>,
                         std::shared_ptr<OperatorPipeline>,
                         std::shared_ptr<OperatorPipeline>*,
                         std::shared_ptr<OperatorPipeline>&>
    {
        /// use PipelineQueryPlanIterator as a fiend to access its state
        friend class PipelineQueryPlanIterator;

    public:
        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        Iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         * @return boolean
         */
        bool operator!=(const Iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return std::shared_ptr<OperatorPipeline>
         */
        std::shared_ptr<OperatorPipeline> operator*();

    private:
        explicit Iterator(const std::shared_ptr<PipelineQueryPlan>& current);
        explicit Iterator();
        std::stack<std::shared_ptr<OperatorPipeline>> workStack;
    };

    /**
     * @brief Starts a new iterator at the start node, which is always a sink.
     * @return iterator
     */
    Iterator begin();

    /**
    * @brief The end of this iterator has an empty work stack.
    * @return iterator
    */
    static Iterator end();

    /**
     * @brief Return a snapshot of the iterator.
     * @return vector<std::shared_ptr<Node>> nodes
     */
    std::vector<std::shared_ptr<OperatorPipeline>> snapshot();

private:
    std::shared_ptr<PipelineQueryPlan> queryPlan;
};
}
