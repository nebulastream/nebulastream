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

#include <Placement/IncrementalPlacementUtils.hpp>

#include <deque>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <NetworkTopology.hpp>
#include <SharedQueryPlan.hpp>

namespace NES
{

std::unordered_map<OperatorId, Host> getPlacement(const LogicalPlan& placedPlan)
{
    std::unordered_map<OperatorId, Host> placement;
    for (const auto& root : placedPlan.getRootOperators())
    {
        for (const auto& op : BFSRange(root))
        {
            const auto trait = op.getTraitSet().tryGet<PlacementTrait>();
            INVARIANT(trait.has_value(), "Operator {} of the placed plan has no placement", op.getId());
            placement.emplace(op.getId(), trait.value()->onNode);
        }
    }
    return placement;
}

std::vector<std::pair<OperatorId, OperatorId>> findNetworkOperatorsForLink(
    const GlobalExecutionPlan& globalExecutionPlan,
    const DistributedQueryId& queryId,
    const Host& upstreamWorker,
    const Host& downstreamWorker)
{
    std::vector<std::pair<OperatorId, OperatorId>> pairs;
    for (const auto& placed : globalExecutionPlan.getChannelsOnLink(queryId, upstreamWorker, downstreamWorker))
    {
        pairs.emplace_back(placed.channel.upstreamOperatorId, placed.channel.downstreamOperatorId);
    }
    return pairs;
}

PinnedOperators findUpstreamAndDownstreamPinnedOperators(
    const SharedQueryPlan& sharedQueryPlan,
    const GlobalExecutionPlan& globalExecutionPlan,
    const Host& upstreamWorker,
    const Host& downstreamWorker,
    const NetworkTopology& topology)
{
    const auto& graph = sharedQueryPlan.getGraph();
    auto pinnedWorkerOf = [&](const OperatorId operatorId)
    {
        const auto worker = sharedQueryPlan.getPinnedWorker(operatorId);
        INVARIANT(worker.has_value(), "Placed operator {} of query {} has no pinned worker", operatorId, sharedQueryPlan.getId());
        return *worker;
    };

    PinnedOperators pinned;
    std::set<OperatorId> toRemove;
    for (const auto& [upstreamOperator, downstreamOperator] :
         findNetworkOperatorsForLink(globalExecutionPlan, sharedQueryPlan.getId(), upstreamWorker, downstreamWorker))
    {
        const auto upstreamOperatorWorker = pinnedWorkerOf(upstreamOperator);
        const auto downstreamOperatorWorker = pinnedWorkerOf(downstreamOperator);

        /// The upstream operator keeps running. This holds as long as only leaf workers change their parent.
        pinned.upstreamOperators.insert(upstreamOperator);

        /// Fixed issue: the original retries the reachability computation in a busy loop until it acquires read locks on all topology
        /// nodes. The topology is a snapshot here, so no locks and no retries are needed.
        /// Like the original, the worker of the upstream operator is reachable from itself.
        auto reachable = topology.findAllDownstreamNodes(upstreamOperatorWorker);
        reachable.insert(upstreamOperatorWorker);

        if (reachable.contains(downstreamOperatorWorker))
        {
            /// Only the network operators between the two pinned operators have to be replaced.
            pinned.downstreamOperators.insert(downstreamOperator);
            continue;
        }

        /// Find the closest downstream operators that are placed on reachable workers.
        std::deque<OperatorId> operatorsToVisit(
            graph.getDownstreamOperators(upstreamOperator).begin(), graph.getDownstreamOperators(upstreamOperator).end());
        std::unordered_set<OperatorId> visitedOperators;
        while (not operatorsToVisit.empty())
        {
            const auto currentOperator = operatorsToVisit.front();
            operatorsToVisit.pop_front();
            if (not visitedOperators.insert(currentOperator).second)
            {
                continue;
            }
            pinned.upstreamOperators.erase(currentOperator);
            if (reachable.contains(pinnedWorkerOf(currentOperator)))
            {
                pinned.downstreamOperators.insert(currentOperator);
                continue;
            }
            /// The operator is not reachable from this upstream operator, even if it is reachable from another one. It is re-placed, and
            /// a further downstream operator that is reachable from this upstream operator is pinned instead.
            pinned.downstreamOperators.erase(currentOperator);
            toRemove.insert(currentOperator);
            for (const auto& parent : graph.getDownstreamOperators(currentOperator))
            {
                operatorsToVisit.push_back(parent);
            }
            for (const auto& child : graph.getUpstreamOperators(currentOperator))
            {
                if (not toRemove.contains(child))
                {
                    pinned.upstreamOperators.insert(child);
                }
            }
        }
    }
    return pinned;
}

}
