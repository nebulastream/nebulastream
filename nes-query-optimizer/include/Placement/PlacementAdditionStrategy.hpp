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
#include <Identifiers/Identifiers.hpp>
#include <Placement/PlacementRemovalStrategy.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Util/Pointers.hpp>
#include <DeploymentContext.hpp>
#include <GlobalExecutionPlan.hpp>
#include <PlacementAmendmentMode.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

struct PlacementAdditionResult
{
    /// Contexts of the new plans (MarkedForDeployment) and of the running plans whose network sinks connect to new receivers
    /// (MarkedForRedeployment).
    std::map<DecomposedQueryId, DeploymentContext> deploymentContexts;
};

/// Places the operators between the pinned upstream and downstream operators of a change log entry and connects them (the original's
/// BasePlacementAdditionStrategy with the bottom-up placement):
/// 1. Path selection: the candidate workers are the workers on paths from the workers of the pinned upstream operators to the workers of
///    the pinned downstream operators.
/// 2. The operators to be placed are placed with the incremental bottom-up placement. Their slots are occupied afterwards; if a worker
///    has no free slot anymore, the addition fails (optimistic mode). In pessimistic mode, the candidate workers are locked during the
///    placement (strict 2PL).
/// 3. Connected operators on the same worker form a new decomposed query plan (MarkedForDeployment).
/// 4. Every logical edge from or to a placed operator, and every edge whose channel chain was removed, is connected by a new chain of
///    network channels with forwarding plans on intermediate workers.
///    - If the upstream operator stays, its running plan is updated to send over the first hop (MarkedForRedeployment, sink update).
///    - If the downstream operator stays, the last hop reuses the channel of the removed chain, so the new sender attaches to the network
///      source of the running plan, which does not change.
///
/// Deviation: the original merges the new operators into the plans of the pinned operators and redeploys them. The current query engine
/// cannot change a running plan, so the plans of the pinned operators keep running; edges between a pinned and a placed operator on the
/// same worker use a channel on the worker itself.
class PlacementAdditionStrategy
{
public:
    PlacementAdditionStrategy(
        GlobalExecutionPlan& globalExecutionPlan,
        SharedPtr<WorkerCatalog> workerCatalog,
        const QueryDecomposer& decomposer,
        QueryOptimizerNetworkConfiguration networkConfiguration,
        PlacementAmendmentMode mode);

    PlacementAdditionResult updateGlobalExecutionPlan(
        SharedQueryPlan& sharedQueryPlan,
        const std::set<OperatorId>& pinnedUpstreamOperators,
        const std::set<OperatorId>& pinnedDownstreamOperators,
        DecomposedQueryPlanVersion version,
        const PlacementRemovalResult& removal);

private:
    GlobalExecutionPlan& globalExecutionPlan; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    SharedPtr<WorkerCatalog> workerCatalog;
    const QueryDecomposer& decomposer; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    QueryOptimizerNetworkConfiguration networkConfiguration;
    PlacementAmendmentMode mode;
};

}
