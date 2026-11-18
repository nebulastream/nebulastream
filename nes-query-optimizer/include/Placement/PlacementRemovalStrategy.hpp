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

#include <map>
#include <set>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <DeploymentContext.hpp>
#include <GlobalExecutionPlan.hpp>
#include <PlacementAmendmentMode.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

/// A logical edge of the global plan of a query: (upstream operator, downstream operator).
using LogicalEdge = std::pair<OperatorId, OperatorId>;

struct PlacementRemovalResult
{
    /// Contexts of the drained plans.
    std::map<DecomposedQueryId, DeploymentContext> deploymentContexts;
    /// The first hop of every removed channel chain that starts at a plan which keeps running. The placement addition updates the network
    /// sink of that plan.
    std::map<LogicalEdge, PlacedNetworkChannel> removedFirstHops;
    /// The last hop of every removed channel chain that ends at a plan which keeps running. The placement addition reuses its channel, so a
    /// new sender attaches to the network source of that plan.
    std::map<LogicalEdge, PlacedNetworkChannel> removedLastHops;
};

/// Removes the invalid placements between the pinned upstream and downstream operators of a change log entry (the original's
/// PlacementRemovalStrategy). Plans that only host operators to be re-placed or removed, and forwarding plans of channel chains that are
/// no longer valid, are drained (MarkedForMigration). The operators to be re-placed become ToBePlaced and release their slots.
///
/// Deviation: the original removes the operators from the plans and gives the remaining plan a new version. The current query engine
/// cannot change a running plan, so a plan is either kept, or drained as a whole. The amendment phase re-places all operators of a plan
/// that hosts operators to be re-placed; the removal throws a PlacementFailure if it still gets a partially affected plan.
class PlacementRemovalStrategy
{
public:
    PlacementRemovalStrategy(GlobalExecutionPlan& globalExecutionPlan, WorkerCatalog& workerCatalog, PlacementAmendmentMode mode);

    PlacementRemovalResult updateGlobalExecutionPlan(
        SharedQueryPlan& sharedQueryPlan,
        const std::set<OperatorId>& pinnedUpstreamOperators,
        const std::set<OperatorId>& pinnedDownstreamOperators,
        DecomposedQueryPlanVersion version);

private:
    GlobalExecutionPlan& globalExecutionPlan; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    WorkerCatalog& workerCatalog; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    PlacementAmendmentMode mode;
};

/// Returns the operators from the pinned upstream to the pinned downstream operators, both included.
std::set<OperatorId> getOperatorsToProcess(
    const SharedQueryPlan& sharedQueryPlan,
    const std::set<OperatorId>& pinnedUpstreamOperators,
    const std::set<OperatorId>& pinnedDownstreamOperators);

}
