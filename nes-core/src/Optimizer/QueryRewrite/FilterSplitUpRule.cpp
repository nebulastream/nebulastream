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
    // If the query plan contains a parentOperators->filter(expression1 && expression2)->childOperator.
    // The plan can be rewritten to parentOperators->filter(expression1)->filter(expression2)->childOperator.
    if (filterOperator->getPredicate()->instanceOf<AndExpressionNode>()){
        NES_DEBUG("Found a conjunctive predicate, split up might be possible");
        NES_DEBUG("Retrieve predicates forming the conjunction");
        std::vector<NodePtr> conjunctivePredicates = filterOperator->getPredicate()->getChildren();
        for (const auto& conjunctivePredicate : conjunctivePredicates){
            NES_DEBUG("Create new filter with one side of the conjunction");
            auto newFilterPredicate = filterOperator->copy()->as<FilterLogicalOperatorNode>();
            newFilterPredicate->setId(Util::getNextOperatorId());
            newFilterPredicate->setPredicate(conjunctivePredicate->as<ExpressionNode>());
            NES_DEBUG("Insert new filter with one part of the conjunction, {}", newFilterPredicate->toString());
            if (!filterOperator->insertBetweenThisAndChildNodes(newFilterPredicate)) {
                throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
                NES_ERROR("FilterSplitUpRule: Error while trying to insert a filterOperator into the queryPlan");
            }
            conjunctivePredicates.push_back(newFilterPredicate);
        }
        NES_DEBUG("Remove old filter with the conjunction");
        if (!filterOperator->removeAndJoinParentAndChildren()) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
            NES_ERROR("FilterSplitUpRule: Error while trying to remove a filterOperator from the queryPlan");
        }
        NES_DEBUG("Newly created filters could also be further split up");
        for (const auto& newFilterPredicate : conjunctivePredicates){
            NES_DEBUG("Try to split up the new filter {} again", newFilterPredicate->toString());
            splitUpFilters(newFilterPredicate->as<FilterLogicalOperatorNode>());
        }
    }
    else if (filterOperator->getPredicate()->instanceOf<NegateExpressionNode>()) {
        NES_DEBUG("Found negated expression, rewrite might be possible");
        if (filterOperator->getPredicate()->getChildren()[0]->instanceOf<OrExpressionNode>()){
            NES_DEBUG("The predicate is of the form !( expression1 || expression2 )");
            NES_DEBUG("It can be reformulated to ( !expression1 && !expression2 )");
            auto orExpression = filterOperator->getPredicate()->getChildren()[0];
            auto negatedChild1 = NegateExpressionNode::create(orExpression->getChildren()[0]->as<ExpressionNode>());
            auto negatedChild2 = NegateExpressionNode::create(orExpression->getChildren()[1]->as<ExpressionNode>());
            auto equivalentAndExpression = AndExpressionNode::create(negatedChild1, negatedChild2);
            NES_DEBUG("Change predicate to equivalent AndExpression");
            filterOperator->setPredicate(equivalentAndExpression);
            NES_DEBUG("Try to split up the new filter again");
            splitUpFilters(filterOperator);
        }
        else if (filterOperator->getPredicate()->getChildren()[0]->instanceOf<NegateExpressionNode>()){
            NES_DEBUG("Found double negation: reformulate predicates in the form (!!expression) to (expression)");
            auto firstNegatedExpression = filterOperator->getPredicate();
            auto secondNegatedExpression = firstNegatedExpression->getChildren()[0];
            auto negatedTwiceExpression = secondNegatedExpression->getChildren()[0];
            NES_DEBUG("Change predicate to equivalent expression without negations");
            filterOperator->setPredicate(negatedTwiceExpression->as<ExpressionNode>()->copy());
            NES_DEBUG("Try to split up the new filter again");
            splitUpFilters(filterOperator);
        }
    }
}

} //end namespace NES::Optimizer
