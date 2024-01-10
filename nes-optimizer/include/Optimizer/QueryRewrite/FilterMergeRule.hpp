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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERMERGERULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERMERGERULE_HPP_

#include <Operators/Expressions/ExpressionNode.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <memory>
#include <set>

namespace NES {
class Node;
using NodePtr = std::shared_ptr<Node>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class FilterLogicalOperatorNode;
using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;
}// namespace NES

namespace NES::Optimizer {

class FilterMergeRule;
using FilterMergeRulePtr = std::shared_ptr<FilterMergeRule>;

/**
 * @brief This rewrite rule identifies sequences of consecutive filters. The filters are combined together
 * into one filter where the predicate is a conjunction of the predicates of the original filters.
 * This reduces the number of operators in the query plan and should provide a performance benefit.
 * It is especially useful after splitting up and pushing down the filters.
 *
 * Example:
 *
 *      |
 *    Filter(a == 1)
 *      |                             |
 *    Filter(b > 0)          ===>   Filter((a == 1) && (b > 0) && (c < 2) && (a > 2)
 *      |                             |
 *    Filter(c < 2)
 *      |
 *    Filter(a > 2)
 *      |
 *
 */

class FilterMergeRule : public BaseRewriteRule {

  public:
    static FilterMergeRulePtr create();
    FilterMergeRule() = default;
    virtual ~FilterMergeRule() = default;

    /**
     * @brief Apply Filter Merge rule on input query plan
     * @param queryPlan: the original query plan
     * @return updated logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    /**
     * @brief Given a filter, retrieve all the consecutive filters (including the filter itself).
     * @param firstFilter: the filter to check
     * @return vector of filters
     */
    static std::vector<FilterLogicalOperatorNodePtr> getConsecutiveFilters(const NES::FilterLogicalOperatorNodePtr& firstFilter);
};
}// namespace NES::Optimizer
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERMERGERULE_HPP_
