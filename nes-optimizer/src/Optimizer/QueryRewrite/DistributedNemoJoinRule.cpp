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

#include <API/Schema.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedNemoJoinRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES::Optimizer
{

DistributedNemoJoinRule::DistributedNemoJoinRule(
    Configurations::OptimizerConfiguration configuration,
    TopologyPtr topology,
    const std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>> & keyDistributionMap)
    : topology(topology), configuration(configuration), keyDistributionMap(keyDistributionMap)
{
}

DistributedNemoJoinRulePtr DistributedNemoJoinRule::create(
    Configurations::OptimizerConfiguration configuration,
    TopologyPtr topology,
    const std::map<Catalogs::Source::SourceCatalogEntryPtr, std::set<uint64_t>> & distributionMap)
{
    NES_DEBUG("DistributedNemoJoinRule: Using distributed nemo join");
    return std::make_shared<DistributedNemoJoinRule>(DistributedNemoJoinRule(configuration, topology, distributionMap));
}

QueryPlanPtr DistributedNemoJoinRule::apply(QueryPlanPtr queryPlan)
{
    NES_DEBUG("DistributedNemoJoinRule: Plan before placement\n{}", queryPlan->toString());
    auto joinOps = queryPlan->getOperatorByType<LogicalJoinOperator>();

    if (!joinOps.empty())
    {
        std::map<uint64_t, std::set<WorkerId>> commonNodes = getNodesWithCommonKeys();
        auto commonOperators = getOperatorsWithCommonKeys(queryPlan, commonNodes);

        if (!commonOperators.empty())
        {
            for (const auto & pair : commonNodes)
            {
                std::string sourceIds;
                for (const auto & catalogEntry : pair.second)
                {
                    sourceIds.append(catalogEntry.toString() + ",");
                }
                NES_DEBUG("DistributedNemoJoinRule: Common nodes {}->{}", pair.first, sourceIds);
            }

            for (const auto & pair : commonOperators)
            {
                std::string sourceIds;
                for (const auto & op : pair.second)
                {
                    sourceIds.append(op->toString() + ",");
                }
                NES_DEBUG("DistributedNemoJoinRule: Common operators {}->{}", pair.first, sourceIds);
            }

            for (const LogicalJoinOperatorPtr & joinOp : joinOps)
            {
                NES_DEBUG("DistributedNemoJoinRule::apply: join operator {}", joinOp->toString());
                auto parents = joinOp->getParents();
                // replace old central join with the new join replicas
                joinOp->removeAllParent();
                joinOp->removeChildren();

                for (const auto & pair : commonOperators)
                {
                    for (auto it1 = pair.second.begin(); it1 != pair.second.end(); ++it1)
                    {
                        for (auto it2 = std::next(it1); it2 != pair.second.end(); ++it2)
                        {
                            auto l_op = it1->get();
                            auto r_op = it2->get();
                            NES_DEBUG("DistributedNemoJoinRule::apply: sub join operator {} <-> {}", l_op->toString(), r_op->toString());

                            LogicalJoinOperatorPtr newJoin
                                = LogicalOperatorFactory::createJoinOperator(joinOp->getJoinDefinition())->as<LogicalJoinOperator>();
                            newJoin->addChild(l_op->getParents()[0]);
                            newJoin->addChild(r_op->getParents()[0]);
                            for (const auto & parent : parents)
                            {
                                parent->addChild(newJoin);
                            }
                        }
                    }
                }
            }
        }
        else
        {
            NES_DEBUG("DistributedNemoJoinRule: No common operators found");
        }
    }
    else
    {
        NES_DEBUG("DistributedNemoJoinRule: No join operator in query");
    }
    NES_DEBUG("DistributedNemoJoinRule: Plan after placement\n{}", queryPlan->toString());

    return queryPlan;
}

std::map<uint64_t, std::set<SourceLogicalOperatorPtr>> DistributedNemoJoinRule::getOperatorsWithCommonKeys(
    const QueryPlanPtr & queryPlan, const std::map<uint64_t, std::set<WorkerId>> & commonKeys)
{
    auto sourceOperators = queryPlan->getSourceOperators();
    std::map<uint64_t, std::set<SourceLogicalOperatorPtr>> commonOperators;
    for (const auto & pair : commonKeys)
    {
        auto key = pair.first;
        auto nodeIds = pair.second;
        for (const SourceLogicalOperatorPtr & op : sourceOperators)
        {
            auto wId = std::any_cast<WorkerId>(op->getProperty(PINNED_WORKER_ID));
            if (nodeIds.contains(wId))
            {
                if (!commonOperators.contains(key))
                {
                    commonOperators[key] = {};
                }
                commonOperators[key].insert(op);
            }
        }
    }
    return commonOperators;
}

std::map<uint64_t, std::set<WorkerId>> DistributedNemoJoinRule::getNodesWithCommonKeys()
{
    std::map<uint64_t, std::set<WorkerId>> result;

    // Iterate through each pair in the map
    for (auto it1 = keyDistributionMap.begin(); it1 != keyDistributionMap.end(); ++it1)
    {
        for (auto it2 = std::next(it1); it2 != keyDistributionMap.end(); ++it2)
        {
            if (it1->first != it2->first)
            {
                // Iterate through integers in the first set
                for (auto num : it1->second)
                {
                    std::set<Catalogs::Source::SourceCatalogEntryPtr> commonEntries;
                    // Check if the second set contains the integer
                    if (it2->second.find(num) != it2->second.end())
                    {
                        // If yes, add it to the common integers set
                        if (!result.contains(num))
                        {
                            result[num] = {};
                        }
                        result[num].emplace(it1->first->getTopologyNodeId());
                        result[num].emplace(it2->first->getTopologyNodeId());
                    }
                }
            }
        }
    }
    return result;
}

} // namespace NES::Optimizer
