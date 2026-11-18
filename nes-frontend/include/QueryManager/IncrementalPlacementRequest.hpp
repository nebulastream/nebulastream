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

#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Pointers.hpp>
#include <DeploymentContext.hpp>
#include <ErrorHandling.hpp>
#include <PlacementAmendmentMode.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <TopologyChangeEvent.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

/// Configuration of the incremental placement and deployment (the original's optimizer configuration switches).
struct IncrementalPlacementConfiguration
{
    /// On: ISQP/ISQD. Off: the holistic baselines, which redeploy every affected query from scratch.
    bool enableIncrementalPlacement = true;
    /// How long the deployment waits for the drained plans of a query to terminate before it gives up tracking them.
    std::chrono::milliseconds drainTimeout = std::chrono::seconds(60);
    /// How concurrent amendments synchronize on the worker capacities.
    PlacementAmendmentMode placementAmendmentMode = PlacementAmendmentMode::Optimistic;
    /// Number of threads amending affected queries in parallel. One processes the queries serially (HSQD-like).
    size_t placementAmendmentThreadCount = 1;
};

/// Deploys the result of a placement amendment of one query (the original's DeploymentPhase).
/// Incremental: starts the new plans, injects the reconfiguration marker at the marker units, and removes the drained plans once they
/// terminated. Holistic: stops the removed plans and starts the new ones.
class DeploymentPhase
{
public:
    DeploymentPhase(QueryManager& queryManager, IncrementalPlacementConfiguration configuration);

    std::expected<void, std::vector<Exception>> execute(const DistributedQueryId& queryId, const DeploymentUnit& deploymentUnit);

    /// Builds the reconfiguration marker of a deployment unit: an update event for every updated plan, a drain event for every drained
    /// plan and an update-and-drain event for every plan that hands its state over (the original's updateReconfigurationMarker).
    static ReconfigurationMarker computeReconfigurationMarker(const DeploymentUnit& deploymentUnit);

private:
    QueryManager& queryManager; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    IncrementalPlacementConfiguration configuration;
};

struct PlacementAmendmentResult
{
    bool success = false;
    std::chrono::microseconds placementTime{0};
    std::chrono::microseconds deploymentTime{0};
};

/// Amends the placement of one affected query and deploys the result (the original's PlacementAmendmentInstance).
class PlacementAmendmentInstance
{
public:
    PlacementAmendmentInstance(
        QueryManager& queryManager,
        SharedPtr<WorkerCatalog> workerCatalog,
        const QueryDecomposer& decomposer,
        QueryOptimizerNetworkConfiguration networkConfiguration,
        IncrementalPlacementConfiguration configuration);

    PlacementAmendmentResult execute(const DistributedQueryId& queryId);

private:
    QueryManager& queryManager; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    SharedPtr<WorkerCatalog> workerCatalog;
    const QueryDecomposer& decomposer; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    QueryOptimizerNetworkConfiguration networkConfiguration;
    IncrementalPlacementConfiguration configuration;
};

/// Amends the affected queries of a request in parallel with a pool of threads (the original's PlacementAmendmentHandler).
class PlacementAmendmentHandler
{
public:
    explicit PlacementAmendmentHandler(size_t numberOfThreads);

    /// Runs the amendment of every query and returns the results in the order of the queries.
    std::vector<PlacementAmendmentResult> process(
        const std::vector<DistributedQueryId>& queries,
        const std::function<PlacementAmendmentResult(const DistributedQueryId&)>& amend) const;

private:
    size_t numberOfThreads;
};

struct IncrementalPlacementResult
{
    size_t numberOfAffectedQueries = 0;
    size_t numberOfFailedAmendments = 0;
    std::chrono::microseconds processingTime{0};
};

/// Processes a batch of topology change events (the original's ISQPRequest): applies them to the topology, finds the queries affected by
/// removed links and nodes, marks the operators to re-place, and amends and deploys every affected query.
///
/// Deviation: the original also batches query additions and removals. The current frontend adds and removes queries with their own
/// statements, so a request only contains topology changes.
class IncrementalPlacementRequest
{
public:
    IncrementalPlacementRequest(
        QueryManager& queryManager,
        SharedPtr<WorkerCatalog> workerCatalog,
        QueryDecomposer decomposer,
        QueryOptimizerNetworkConfiguration networkConfiguration,
        IncrementalPlacementConfiguration configuration);

    IncrementalPlacementResult execute(const std::vector<TopologyChangeEvent>& events);

private:
    QueryManager& queryManager; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    SharedPtr<WorkerCatalog> workerCatalog;
    QueryDecomposer decomposer;
    QueryOptimizerNetworkConfiguration networkConfiguration;
    IncrementalPlacementConfiguration configuration;
};

}
