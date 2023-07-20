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
    std::vector<ExpressionNodePtr> splitPredicates = filterOperator->splitUpPredicates();
    if (splitPredicates.size() < 2){
        NES_DEBUG("The filter predicates of filter {} cannot be split", filterOperator->toString());
    }
    else{
        std::vector<FilterLogicalOperatorNodePtr> filterPredicates = {};
        for (auto& predicate : splitPredicates){
            auto filterPredicate = filterOperator->copy()->as<FilterLogicalOperatorNode>();
            filterPredicate->setId(Util::getNextOperatorId());
            filterPredicate->setPredicate(predicate);
            NES_DEBUG("Insert new filter with one part of the conjunction, {}", filterPredicate->toString());
            if (!filterOperator->insertBetweenThisAndChildNodes(filterPredicate)) {
                throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
                NES_ERROR("FilterSplitUpRule: Error while trying to insert a filterOperator into the queryPlan");
            }
            filterPredicates.push_back(filterPredicate);
        }
        NES_DEBUG("Remove old filter with the conjunction");
        if (!filterOperator->removeAndJoinParentAndChildren()) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
            NES_ERROR("FilterSplitUpRule: Error while trying to remove a filterOperator from the queryPlan");
        }
        NES_DEBUG("Newly created filters could also be further split up");
        for (auto& filterPredicate : filterPredicates){
            splitUpFilters(filterPredicate);
        }
    }
}

} //end namespace NES::Optimizer
