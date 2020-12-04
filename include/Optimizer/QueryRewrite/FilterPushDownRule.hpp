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

#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWN_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWN_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class FilterPushDownRule;
typedef std::shared_ptr<FilterPushDownRule> FilterPushDownRulePtr;

/**
 * @brief This class is responsible for altering the query plan to push down the filter as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the filter can't be push below a source node.
 *  2.) If their exists a map/window operator that manipulates the field used in the predicate of the filter
 *      then, the filter can't be pushed down below the map/window.
 *  3.) If their exists another filter operator next to the target filter operator then it can't be pushed down below that
 *      specific filter operator.
 */
class FilterPushDownRule : public BaseRefinementRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) override;

    static FilterPushDownRulePtr create();

  private:
    explicit FilterPushDownRule();

    /**
     * @brief Push down given filter operator as close to the source operator as possible
     * @param filterOperator
     */
    void pushDownFilter(FilterLogicalOperatorNodePtr filterOperator);

    /**
     * @brief Get the name of the field manipulated by the Map operator
     * @param Operator pointer
     * @return name of the field
     */
    std::string getFieldNameUsedByMapOperator(NodePtr node) const;

    /**
     * @brief Validate if the input field is used in the filter predicate of the operator
     * @param filterOperator : filter operator whose predicate need to be checked
     * @param fieldName :  name of the field to be checked
     * @return true if field use in the filter predicate else false
     */
    bool isFieldUsedInFilterPredicate(FilterLogicalOperatorNodePtr filterOperator, const std::string fieldName) const;
};

}// namespace NES

#endif//NES_IMPL_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWN_HPP_
