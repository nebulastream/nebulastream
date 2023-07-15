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
            //method calls itself recursively until it can not push the filter further down(upstream).
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
    if (filterOperator->getPredicate()->instanceOf<AndExpressionNode>()){
        auto copy1 = filterOperator->copy()->as<FilterLogicalOperatorNode>();
        copy1->setId(Util::getNextOperatorId());
        copy1->setPredicate(filterOperator->getPredicate()->getChildren()[0]->as<ExpressionNode>());

        auto copy2 = filterOperator->copy()->as<FilterLogicalOperatorNode>();
        copy2->setId(Util::getNextOperatorId());
        copy2->setPredicate(filterOperator->getPredicate()->getChildren()[1]->as<ExpressionNode>());

        if (!filterOperator->insertBetweenThisAndChildNodes(copy1)) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
        }
        if (!copy1->insertBetweenThisAndChildNodes(copy2)) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
        }

        if (!filterOperator->removeAndJoinParentAndChildren()) {
            throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
        }

        splitUpFilters(copy1);
        splitUpFilters(copy2);
    }
}


}
