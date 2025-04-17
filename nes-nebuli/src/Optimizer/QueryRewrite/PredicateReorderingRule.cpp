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

#include <memory>
#include <vector>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Optimizer/QueryRewrite/PredicateReorderingRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer
{

std::shared_ptr<PredicateReorderingRule> PredicateReorderingRule::create()
{
    return std::make_shared<PredicateReorderingRule>();
}

std::shared_ptr<QueryPlan> PredicateReorderingRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    std::set<OperatorId> visitedOperators;
    auto filterOperators = queryPlan->getOperatorByType<SelectionLogicalOperator>();
    NES_DEBUG("PredicateReorderingRule: Identified {} filter nodes in the query plan", filterOperators.size());
    NES_DEBUG("Query before applying the rule: {}", queryPlan->toString());
    for (auto& filter : filterOperators)
    {
        if (visitedOperators.find(filter->getId()) == visitedOperators.end())
        {
            std::vector<std::shared_ptr<SelectionLogicalOperator>> consecutiveFilters = getConsecutiveFilters(filter);
            NES_TRACE(
                "PredicateReorderingRule: Filter {} has {} consecutive filters as children", filter->getId(), consecutiveFilters.size());
            if (consecutiveFilters.size() >= 2)
            {
                const std::vector<std::shared_ptr<Operator>> filterChainParents = consecutiveFilters.front()->parents;
                const std::vector<std::shared_ptr<Operator>> filterChainChildren = consecutiveFilters.back()->children;
                NES_TRACE("PredicateReorderingRule: If the filters are already sorted, no change is needed");
                auto already_sorted = std::is_sorted(
                    consecutiveFilters.begin(),
                    consecutiveFilters.end(),
                    [](const std::shared_ptr<SelectionLogicalOperator>& lhs, const std::shared_ptr<SelectionLogicalOperator>& rhs)
                    { return lhs->getSelectivity() < rhs->getSelectivity(); });
                if (!already_sorted)
                {
                    NES_TRACE("PredicateReorderingRule: Sort all filter nodes in increasing order of selectivity");
                    std::sort(
                        consecutiveFilters.begin(),
                        consecutiveFilters.end(),
                        [](const std::shared_ptr<SelectionLogicalOperator>& lhs, const std::shared_ptr<SelectionLogicalOperator>& rhs)
                        { return lhs->getSelectivity() < rhs->getSelectivity(); });
                    NES_TRACE("PredicateReorderingRule: Start re-writing the new query plan");
                    NES_TRACE("PredicateReorderingRule: Remove parent/children references");
                    for (auto& filterToReassign : consecutiveFilters)
                    {
                        filterToReassign->removeAllParent();
                        filterToReassign->removeChildren();
                    }
                    NES_TRACE("PredicateReorderingRule: For each filter, reassign children according to new order");
                    for (unsigned int i = 0; i < consecutiveFilters.size() - 1; i++)
                    {
                        auto filterToReassign = consecutiveFilters.at(i);
                        filterToReassign->addChild(consecutiveFilters.at(i + 1));
                    }
                    NES_TRACE("PredicateReorderingRule: Retrieve most and least selective filters");
                    auto leastSelectiveFilter = consecutiveFilters.front();
                    auto mostSelectiveFilter = consecutiveFilters.back();
                    NES_TRACE("PredicateReorderingRule: Least selective filter goes to the top (closer to sink), his new "
                              "parents will be the chain parents'");
                    for (auto& filterChainParent : filterChainParents)
                    {
                        leastSelectiveFilter->addParent(filterChainParent);
                    }
                    NES_TRACE("PredicateReorderingRule: Most selective filter goes to the bottom (closer to the source), his "
                              "new children will be the chain children");
                    for (auto& filterChainChild : filterChainChildren)
                    {
                        mostSelectiveFilter->addChild(filterChainChild);
                    }
                }
                NES_TRACE("PredicateReorderingRule: Mark the involved nodes as visited");
                for (auto& orderedFilter : consecutiveFilters)
                {
                    visitedOperators.insert(orderedFilter->getId());
                }
            }
        }
        else
        {
            NES_TRACE("PredicateReorderingRule: Filter node already visited");
        }
    }
    NES_DEBUG("Query after applying the rule: {}", queryPlan->toString());
    return queryPlan;
}

std::vector<std::shared_ptr<SelectionLogicalOperator>> PredicateReorderingRule::getConsecutiveFilters(const std::shared_ptr<SelectionLogicalOperator>& filter)
{
    std::vector<std::shared_ptr<SelectionLogicalOperator>> consecutiveFilters = {};
    DFSRange queryPlanNodeIterator(filter);
    auto nodeIterator = queryPlanNodeIterator.begin();
    auto node = (*nodeIterator);
    while (NES::Util::instanceOf<SelectionLogicalOperator>(node))
    {
        NES_DEBUG("Found consecutive filter in the chain, adding it the list");
        consecutiveFilters.push_back(NES::Util::as<SelectionLogicalOperator>(node));
        ++nodeIterator;
        node = (*nodeIterator);
    }
    NES_DEBUG("Found {} consecutive filters", consecutiveFilters.size());
    return consecutiveFilters;
}

}
