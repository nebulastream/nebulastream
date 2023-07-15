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

#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/FilterSplitUpRule.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES::Optimizer {

FilterSplitUpRulePtr FilterSplitUpRule::create() { return std::make_shared<FilterSplitUpRule>(FilterSplitUpRule()); }

FilterSplitUpRule::FilterSplitUpRule() = default;

QueryPlanPtr FilterSplitUpRule::apply(NES::QueryPlanPtr queryPlan) {

    NES_INFO("Applying FilterSplitUpRule to query {}", queryPlan->toString());
    const std::vector<OperatorNodePtr> rootOperators = queryPlan->getRootOperators();
    std::set<FilterLogicalOperatorNodePtr> filterOperatorsSet;
    for (const OperatorNodePtr& rootOperator: rootOperators){
        std::vector<FilterLogicalOperatorNodePtr> filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
        filterOperatorsSet.insert(filters.begin(), filters.end());
    }
    std::vector<FilterLogicalOperatorNodePtr> filterOperators(filterOperatorsSet.begin(), filterOperatorsSet.end());
    NES_DEBUG("FilterSplitUpRule: Sort all filter nodes in increasing order of the operator id")
    std::sort(filterOperators.begin(),
              filterOperators.end(),
              [](const FilterLogicalOperatorNodePtr& lhs, const FilterLogicalOperatorNodePtr& rhs) {
                  return lhs->getId() < rhs->getId();
              });
    auto originalQueryPlan = queryPlan->copy();
    try {
        NES_DEBUG("FilterSplitUpRule: Iterate over all the filter operators to split them");
        for (FilterLogicalOperatorNodePtr filterOperator : filterOperators) {
            splitUpFilters(filterOperator);
        }
        return queryPlan;
    } catch (std::exception& exc) {
        NES_ERROR("FilterSplitUpRule: Error while applying FilterSplitUpRule: {}", exc.what());
        NES_ERROR("FilterSplitUpRule: Returning unchanged original query plan {}", originalQueryPlan->toString());
        return originalQueryPlan;
    }
}

void FilterSplitUpRule::splitUpFilters(FilterLogicalOperatorNodePtr filterOperator) {
    // if our query plan contains a parentOperaters->filter(expression1 && expression2)->childOperator.
    // We can rewrite this plan to parentOperaters->filter(expression1)->filter(expression2)->childOperator.
    if (filterOperator->getPredicate()->instanceOf<AndExpressionNode>()){
        //create filter that contains expression1 of the andExpression
        auto child1 = filterOperator->copy()->as<FilterLogicalOperatorNode>();
        child1->setId(Util::getNextOperatorId());
        child1->setPredicate(filterOperator->getPredicate()->getChildren()[0]->as<ExpressionNode>());

        //create filter that contains expression2 of the andExpression
        auto child2 = filterOperator->copy()->as<FilterLogicalOperatorNode>();
        child2->setId(Util::getNextOperatorId());
        child2->setPredicate(filterOperator->getPredicate()->getChildren()[1]->as<ExpressionNode>());

        // insert new filter with expression1 of the andExpression
        if (!filterOperator->insertBetweenThisAndChildNodes(child1)) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
            NES_ERROR("FilterSplitUpRule: Error while trying to insert a filterOperator into the queryPlan");
        }
        // insert new filter with expression2 of the andExpression
        if (!child1->insertBetweenThisAndChildNodes(child2)) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
            NES_ERROR("FilterSplitUpRule: Error while trying to insert a filterOperator into the queryPlan");
        }
        // remove old filter that had the andExpression
        if (!filterOperator->removeAndJoinParentAndChildren()) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
            NES_ERROR("FilterSplitUpRule: Error while trying to remove a filterOperator from the queryPlan");
        }

        // newly created filters could also be andExpressions that can be further split up
        splitUpFilters(child1);
        splitUpFilters(child2);

    }
    // In the case that we have a predicate !( expression1 || expression2 ) we can reformulate it to ( !expression1 && !expression2 ).
    // We can use the reformulated predicate to split up the filter.
    else if (filterOperator->getPredicate()->instanceOf<NegateExpressionNode>()) {
        if (filterOperator->getPredicate()->getChildren()[0]->instanceOf<OrExpressionNode>()){
            auto orExpression = filterOperator->getPredicate()->getChildren()[0];
            auto negatedChild1 = NegateExpressionNode::create(orExpression->getChildren()[0]->as<ExpressionNode>());
            auto negatedChild2 = NegateExpressionNode::create(orExpression->getChildren()[1]->as<ExpressionNode>());

            auto equivalentAndExpression = AndExpressionNode::create(negatedChild1, negatedChild2);
            filterOperator->setPredicate(equivalentAndExpression); //changing predicate to equivalent AndExpression

            splitUpFilters(filterOperator); //splitting up the filter
        }
    }
}

} //end namespace NES::Optimizer
