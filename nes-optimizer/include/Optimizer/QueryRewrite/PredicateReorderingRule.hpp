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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_PREDICATEREORDERINGRULE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_PREDICATEREORDERINGRULE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
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

class PredicateReorderingRule;
using PredicateReorderingRulePtr = std::shared_ptr<PredicateReorderingRule>;

/**
 * @brief This rewrite rule identifies chains of adjacent predicates with various expected cardinalities.
 * The adjacent predicates are sorted and executed such that the predicates with a high selectivity are executed first.
 * This rule can reduce the size of intermediate results.
 *
 * Example:
 *
 * SELECT   ve.vehicle_id, ve.model_name
 * FROM     vehicles ve,
 *          orders   od
 * WHERE    ve.vehicle_id = od.vehicle_id
 *   	   AND ve.engine_hp > 70  -- High selectivity estimated
 *          AND od.order_date > DATE(NOW() - interval 16 day)
 * GROUP BY ve.vehicle_id, ve.model_name;
 *
 *  The estimated cardinality of the predicate P1 (ve.engine_hp > 70) which selects all vehicles with at least a
 *  basic horsepower of 70 is high with an estimated selectivity of about 0.9.
 *  The estimated cardinality of the predicate P2 (od.order_date > DATE(NOW() - interval 16 day)) which limits the records
 *  fetched to the previous 15 days is low with an estimated selectivity of just 0.1.
 *  The rule will execute the predicate P1 with high selectivity first and potentially reduce the records going
 *  to the P2 significantly.
 *
 */

class PredicateReorderingRule : public BaseRewriteRule {

  public:
    static PredicateReorderingRulePtr create();
    PredicateReorderingRule() = default;
    virtual ~PredicateReorderingRule() = default;

    /**
     * @brief Apply Predicate Reordering rule on input query plan
     * @param queryPlan: the original query plan
     * @return updated logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    /**
     * @brief Given a node, check if the parent or the child is a filter.
     * @param OperatorNodePtr: the node to be check
     * @return boolean, true when a consecutive filter is found
     */
    static std::vector<FilterLogicalOperatorNodePtr> getConsecutiveFilters(const NES::FilterLogicalOperatorNodePtr& firstFilter);
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYREWRITE_PREDICATEREORDERINGRULE_HPP_
