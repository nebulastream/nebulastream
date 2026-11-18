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

#include <compare>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <GlobalExecutionPlan.hpp>

namespace NES
{

/// What a placement amendment changes for one decomposed query plan (the original's DeploymentContext).
/// The state of the plan says what happens: MarkedForDeployment (deploy a new plan), MarkedForRedeployment (update a running plan to the
/// new version), MarkedForMigration (drain a running plan), MarkedForUpdateAndDrain (hand the state over, then drain).
struct DeploymentContext
{
    /// The plan with its new version and deployment state.
    DecomposedQueryPlan plan;
    /// The version that currently runs on the worker, for plans that are updated or drained.
    std::optional<DecomposedQueryPlanVersion> oldVersion;
    /// The plan belongs to an ad-hoc state migration query.
    bool forMigration = false;
    /// For updated plans: the network sinks that connect to a new receiver.
    std::vector<NetworkSinkUpdate> sinkUpdates;
    /// For drained plans: the outgoing channels whose receiving plan keeps running. Their network sinks detach instead of forwarding the
    /// reconfiguration marker, so a new sender can attach to the receiver.
    std::vector<std::string> detachedChannels;
};

/// A decomposed query plan at which the reconfiguration marker is injected (the original's ReconfigurationMarkerUnit).
struct ReconfigurationMarkerUnit
{
    Host worker;
    DecomposedQueryId plan{DecomposedQueryId::INVALID};

    auto operator<=>(const ReconfigurationMarkerUnit&) const = default;
};

/// A stateful operator that moves to another worker. Its state is migrated by an ad-hoc state migration query.
struct StateMigration
{
    OperatorId operatorId{INVALID_OPERATOR_ID};
    Host oldWorker;
    DecomposedQueryId oldPlan{DecomposedQueryId::INVALID};
    Host newWorker;
    DecomposedQueryId newPlan{DecomposedQueryId::INVALID};

    bool operator==(const StateMigration&) const = default;
};

/// The result of a placement amendment (the original's DeploymentUnit).
struct DeploymentUnit
{
    /// Contexts of the holistic redeployment that are removed before the new plans are deployed.
    std::vector<DeploymentContext> deploymentRemovalContexts;
    std::vector<DeploymentContext> deploymentAdditionContexts;
    std::set<ReconfigurationMarkerUnit> reconfigurationMarkerUnits;
    std::vector<StateMigration> stateMigrations;

    [[nodiscard]] bool containsDeploymentContext() const
    {
        return not deploymentRemovalContexts.empty() or not deploymentAdditionContexts.empty();
    }
};

}
