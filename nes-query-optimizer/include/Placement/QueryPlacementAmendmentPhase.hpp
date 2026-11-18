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
#include <Placement/QueryDecomposition.hpp>
#include <Util/Pointers.hpp>
#include <ChangeLog.hpp>
#include <DeploymentContext.hpp>
#include <GlobalExecutionPlan.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

/// Computes the placement changes of one query and the resulting deployment contexts (the original's QueryPlacementAmendmentPhase).
///
/// Incremental mode (ISQP/ISQD): every compacted change log entry of the query is processed by the placement removal and addition. The
/// reconfiguration marker is injected at the plans of the pinned upstream operators and at the first plan after a removed link. Stateful
/// operators (joins) that move get a state migration. Failed entries are recorded again and the query becomes PartiallyProcessed.
///
/// Holistic mode (the baselines HSQD/HCQD): all plans of the query are removed, the query is placed again from scratch and all its plans
/// are deployed anew.
class QueryPlacementAmendmentPhase
{
public:
    QueryPlacementAmendmentPhase(
        GlobalExecutionPlan& globalExecutionPlan,
        SharedPtr<WorkerCatalog> workerCatalog,
        const QueryDecomposer& decomposer,
        QueryOptimizerNetworkConfiguration networkConfiguration,
        bool enableIncrementalPlacement);

    /// Holistic mode replaces the shared query plan, as the query gets new operators.
    DeploymentUnit execute(SharedQueryPlan& sharedQueryPlan);

private:
    DeploymentUnit executeIncrementally(SharedQueryPlan& sharedQueryPlan);
    DeploymentUnit executeHolistically(SharedQueryPlan& sharedQueryPlan);

    /// Computes the plans at which the reconfiguration marker is injected for a change log entry:
    /// Cond-1: the plans hosting the pinned upstream operators.
    /// Cond-2: the first plan after a removed link on the channel chains from the pinned upstream operators to their downstream operators,
    ///         as the marker cannot travel over the removed link.
    void computeReconfigurationMarkerDeploymentUnit(
        const SharedQueryPlan& sharedQueryPlan, const ChangeLogEntry& entry, std::set<ReconfigurationMarkerUnit>& units) const;

    GlobalExecutionPlan& globalExecutionPlan; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    SharedPtr<WorkerCatalog> workerCatalog;
    const QueryDecomposer& decomposer; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    QueryOptimizerNetworkConfiguration networkConfiguration;
    bool enableIncrementalPlacement;
};

}
