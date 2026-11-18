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

#pragma once

#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <GlobalExecutionPlan.hpp>
#include <NetworkTopology.hpp>
#include <SharedQueryPlan.hpp>

namespace NES
{

/// Returns the worker of every operator of a placed plan, taken from the placement traits.
std::unordered_map<OperatorId, Host> getPlacement(const LogicalPlan& placedPlan);

/// The operators that stay where they are when a topology link is removed. All operators between them are re-placed.
struct PinnedOperators
{
    std::set<OperatorId> upstreamOperators;
    std::set<OperatorId> downstreamOperators;

    bool operator==(const PinnedOperators&) const = default;
};

/// Returns the pairs of logical operators (upstream, downstream) whose channels use the link from `upstreamWorker` to `downstreamWorker`.
/// A forwarding hop on an intermediate worker is mapped to the operators at the ends of its channel chain.
std::vector<std::pair<OperatorId, OperatorId>> findNetworkOperatorsForLink(
    const GlobalExecutionPlan& globalExecutionPlan,
    const DistributedQueryId& queryId,
    const Host& upstreamWorker,
    const Host& downstreamWorker);

/// Computes the pinned operators of a query after the link from `upstreamWorker` to `downstreamWorker` was removed (ISQP, Alg. 1).
/// The upstream operator of every channel using the link stays pinned. Its downstream operator stays pinned if its worker can still be
/// reached from the worker of the upstream operator in the changed topology. Otherwise, the closest downstream operators on reachable
/// workers are pinned; the operators in between are re-placed, and the upstream operators of re-placed operators are pinned as well.
PinnedOperators findUpstreamAndDownstreamPinnedOperators(
    const SharedQueryPlan& sharedQueryPlan,
    const GlobalExecutionPlan& globalExecutionPlan,
    const Host& upstreamWorker,
    const Host& downstreamWorker,
    const NetworkTopology& topology);

}
