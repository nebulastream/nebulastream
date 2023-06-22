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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWNRULE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWNRULE_HPP_

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
 *  2.) Every operator below a filter has it's own set of rules that decide if and how the filter can be pushed below that operator
 */
class FilterPushDownRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static FilterPushDownRulePtr create();
    virtual ~FilterPushDownRule() = default;

  private:
    explicit FilterPushDownRule();

    /**
     * @brief Push down given filter operator as close to the source operator as possible, by calling this method recursively
     * @param filterOperator that is pushed down the queryPlan
     * @param curOperator the operator through which we want to push the filter.
     * @param parOperator the operator that is the parent of curOperator in this queryPlan
     */
    void pushDownFilter(FilterLogicalOperatorNodePtr filterOperator, NodePtr curOperator, NodePtr parOperator);

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

    /**
     * In case we can't push the filter any further we call this method to remove the filter from its original position in the query plan
     * and insert the filter at the new position of the query plan. (only if the position of the filter changed)
     * @param filterOperator the filter operator that we want to insert into the query plan
     * @param childOperator we want to insert the filter operator above this operator in the query plan
     * @param parOperator  we want to insert the filter operator below this operator in the query plan
     */
    static void insertFilterIntoNewPosition(FilterLogicalOperatorNodePtr filterOperator,
                                            NodePtr childOperator,
                                            NodePtr parOperator);
};

}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_FILTERPUSHDOWNRULE_HPP_
