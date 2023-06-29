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

#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/PredicateReorderingRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

PredicateReorderingRulePtr PredicateReorderingRule::create() { return std::make_shared<PredicateReorderingRule>(); }

QueryPlanPtr PredicateReorderingRule::apply(NES::QueryPlanPtr queryPlan) {
    std::set<NodeId> visitedNodesIds;
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    NES_DEBUG2("PredicateReorderingRule: Identified {} filter nodes in the query plan", filterOperators.size());
    NES_DEBUG2("Query before applying the rule: ", queryPlan->toString());
    for (auto& filter : filterOperators) {
        if(visitedNodesIds.find(filter->getId()) == visitedNodesIds.end() ) {
            std::vector<FilterLogicalOperatorNodePtr> consecutiveFilters = getConsecutiveFilters(filter);
            NES_TRACE2("PredicateReorderingRule: Filter {} has {} consecutive filters as children", filter->getId(), consecutiveFilters.size());
            if (consecutiveFilters.size() >= 2){
                NES_TRACE2("PredicateReorderingRule: Copy consecutive filters");
                std::vector<FilterLogicalOperatorNodePtr> orderedFilters(consecutiveFilters.begin(), consecutiveFilters.end());
                NES_TRACE2("PredicateReorderingRule: Sort all filter nodes in increasing order of selectivity");
                std::sort(orderedFilters.begin(),
                          orderedFilters.end(),
                          [](const FilterLogicalOperatorNodePtr& lhs, const FilterLogicalOperatorNodePtr& rhs) {
                              return lhs->getSelectivity() < rhs->getSelectivity();
                          });
                NES_TRACE2("PredicateReorderingRule: Start re-writing the new query plan");
                NES_TRACE2("PredicateReorderingRule: Least selective filter goes to the top (close to the sink), his new parents will be the first filter parents'");
                std::vector<NodePtr> filterChainParent = consecutiveFilters.at(0)->getParents();
                std::vector<NodePtr> filterChainChildren = consecutiveFilters.back()->getChildren();
                orderedFilters.at(0)->removeAllParent();
                for(auto& firstFilterParent : filterChainParent){
                     orderedFilters.at(0)->addParent(firstFilterParent);
                }
                NES_TRACE2("PredicateReorderingRule: For each filter, reassign parents according to new order");
                for (unsigned int i = 1; i < orderedFilters.size(); i++) {
                    auto filterToReassign =  orderedFilters.at(i);
                    std::shared_ptr<OperatorNode> filterToUpdate = queryPlan->getOperatorWithId(filterToReassign->getId());
                    if(filterToUpdate != nullptr){
                        filterToUpdate->removeAllParent();
                        filterToUpdate->addParent(orderedFilters.at(i-1));
                    } else{
                        NES_TRACE2("PredicateReorderingRule: Filter to update not found in the plan, something is wrong");
                        continue;
                    }
                }
                NES_TRACE2("PredicateReorderingRule: Most selective filter goes to the bottom (close to the source), his new children will be the last filter children'");
                orderedFilters.back()->removeChildren();
                for (auto& previousChild : filterChainChildren){
                    orderedFilters.back()->addChild(previousChild);
                }
                NES_TRACE2("PredicateReorderingRule: Mark the involved nodes as visited");
                for (auto& orderedFilter : orderedFilters){
                    visitedNodesIds.insert(orderedFilter->getId());
                }
            }
            else{
                NES_TRACE2("PredicateReorderingRule: No consecutive filters found");
            }
        } else{
            NES_TRACE2("PredicateReorderingRule: Filter node already visited");
        }
    }
    NES_DEBUG2("Query after applying the rule: ", queryPlan->toString());
    return queryPlan;
}

std::vector<FilterLogicalOperatorNodePtr> PredicateReorderingRule::getConsecutiveFilters(const NES::FilterLogicalOperatorNodePtr& filter){
    std::vector<FilterLogicalOperatorNodePtr> consecutiveFilters = {};
    DepthFirstNodeIterator queryPlanNodeIterator(filter);
    auto nodeIterator = queryPlanNodeIterator.begin();
    auto node = (*nodeIterator);
    while (node->instanceOf<FilterLogicalOperatorNode>()){
        NES_DEBUG2("Found consecutive filter in the chain, adding it the list");
        consecutiveFilters.push_back(node->as<FilterLogicalOperatorNode>());
        ++nodeIterator;
        node = (*nodeIterator);
    }
    NES_DEBUG2("Found {} consecutive filters", consecutiveFilters.size());
    return consecutiveFilters;
}

}// namespace NES::Optimizer
