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
#include <Operators/Operator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES
{
/**
 * @brief Iterator for query plans, which correctly handles multiple sources and sinks.
 * The iterator visits each operator exactly one time in the following order:
 * top-to-bottom and left-to-right
 *
 * Example Query Plan:
 *
 * -- Sink 1 ---                            --- Source 1 ---
 *              \                         /
 *                --- Filter --- Join ---
 *              /                         \
 * -- Sink 2 ---                            --- Source 2 ---
 *
 * Iteration order:
 * #1 - Sink 1
 * #2 - Sink 2
 * #3 - Filter
 * #4 - Join
 * #5 - Source 1
 * #6 - Source 2
 */
class PlanIterator
{
public:
    explicit PlanIterator(const QueryPlan& queryPlan);
    explicit PlanIterator(const DecomposedQueryPlan& decomposedQueryPlan);

    class Iterator : public std::iterator<std::forward_iterator_tag, std::shared_ptr<Node>, std::shared_ptr<Node>>
    {
        friend class PlanIterator;

    public:
        Iterator& operator++();
        bool operator!=(const Iterator& other) const;
        std::shared_ptr<Node> operator*();

    private:
        explicit Iterator(const std::vector<std::shared_ptr<Operator>>& rootOperators);
        explicit Iterator();
        std::stack<std::shared_ptr<Node>> workStack;
    };

    /// @note always a sink
    Iterator begin();
    static Iterator end();

    /// Returns a snapshot of the iterator.
    std::vector<std::shared_ptr<Node>> snapshot();

private:
    std::vector<std::shared_ptr<Operator>> rootOperators;
};

}
