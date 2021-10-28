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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_FILTER_PUSH_DOWN_RULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_FILTER_PUSH_DOWN_RULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class FilterLogicalOperatorNode;
using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;
}// namespace NES

namespace NES::Optimizer {

class FilterPushDownRule;
using FilterPushDownRulePtr = std::shared_ptr<FilterPushDownRule>;

/**
 * @brief This class is responsible for altering the query plan to push down the filter as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the filter can't be push below a source node.
 *  2.) If their exists a map/window operator that manipulates the field used in the predicate of the filter
 *      then, the filter can't be pushed down below the map/window.
 *  3.) If their exists another filter operator next to the target filter operator then it can't be pushed down below that
 *      specific filter operator.
 */
class FilterPushDownRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static FilterPushDownRulePtr create();
    virtual ~FilterPushDownRule() = default;

  private:
    explicit FilterPushDownRule();

    /**
     * @brief Push down given filter operator as close to the source operator as possible
     * @param filterOperator
     */
    void pushDownFilter(const FilterLogicalOperatorNodePtr& filterOperator);

    /**
     * @brief Get the name of the field manipulated by the Map operator
     * @param Operator pointer
     * @return name of the field
     */
    static std::string getFieldNameUsedByMapOperator(const NodePtr& node);

    /**
     * @brief Validate if the input field is used in the filter predicate of the operator
     * @param filterOperator : filter operator whose predicate need to be checked
     * @param fieldName :  name of the field to be checked
     * @return true if field use in the filter predicate else false
     */
    static bool isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr const& filterOperator, std::string const& fieldName);
};

}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_FILTER_PUSH_DOWN_RULE_HPP_
