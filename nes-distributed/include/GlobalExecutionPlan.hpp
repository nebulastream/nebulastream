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

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Formatter.hpp>
#include <DistributedLogicalPlan.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Identifies a decomposed query plan (DQP), i.e., the part of a query that runs as one local query on one worker.
using DecomposedQueryId = NESStrongType<uint64_t, struct DecomposedQueryId_, 0, 1>;
/// Version of a decomposed query plan. Every placement amendment uses a new version for the plans it changes.
using DecomposedQueryPlanVersion = NESStrongType<uint64_t, struct DecomposedQueryPlanVersion_, 0, 1>;

/// State of a decomposed query plan (the subset of the original's `QueryState` that applies to decomposed query plans).
/// The `MarkedFor*` states are the deployment context states computed by a placement amendment.
enum class DecomposedQueryPlanState : uint8_t
{
    /// A new plan that has to be deployed.
    MarkedForDeployment,
    /// A running plan that is updated to a new version (marker action Update).
    MarkedForRedeployment,
    /// A running plan that is drained and removed (marker action Drain).
    MarkedForMigration,
    /// A running plan with stateful operators that move: hand the state over, then drain (marker action UpdateAndDrain).
    MarkedForUpdateAndDrain,
    Running,
    /// Being drained or migrating its state.
    Migrating,
    Stopped,
};

struct DecomposedQueryPlan
{
    DecomposedQueryId id{DecomposedQueryId::INVALID};
    DecomposedQueryPlanVersion version{DecomposedQueryPlanVersion::INVALID};
    Host worker;
    LogicalPlan plan;
    /// The operators of the global plan of the query that this plan hosts (the original's PLACED_DECOMPOSED_PLAN_ID).
    std::set<OperatorId> hostedOperators;
    DecomposedQueryPlanState state = DecomposedQueryPlanState::Running;
    /// The id of the local query running this plan on the worker, once it has been deployed.
    std::optional<LocalQueryId> localQueryId;
};

/// A network channel between two decomposed query plans of a query.
struct PlacedNetworkChannel
{
    NetworkChannel channel;
    /// The plan holding the network sink of the channel.
    DecomposedQueryId upstreamPlan{DecomposedQueryId::INVALID};
    /// The plan holding the network source of the channel.
    DecomposedQueryId downstreamPlan{DecomposedQueryId::INVALID};

    bool operator==(const PlacedNetworkChannel&) const = default;
};

/// The decomposed query plans of all queries and the network channels between them.
/// The original keeps the plans per execution node and synchronizes on locks per node. The current code base has no execution nodes, so
/// the plans are kept per query. Every placement amendment only changes the plans of its own query, so a single mutex suffices.
/// All accessors return copies, like the original's `getCopyOf*` accessors.
class GlobalExecutionPlan
{
public:
    /// Registers the local plans of a query placed by the initial placement as running decomposed query plans of the first version.
    /// Returns the ids of the created plans per worker, in the order of the local plans.
    std::unordered_map<Host, std::vector<DecomposedQueryId>>
    addQuery(const DistributedQueryId& queryId, const DistributedLogicalPlan& plan);
    void removeQuery(const DistributedQueryId& queryId);
    [[nodiscard]] bool contains(const DistributedQueryId& queryId) const;

    [[nodiscard]] std::vector<DecomposedQueryPlan> getDecomposedQueryPlans(const DistributedQueryId& queryId) const;
    [[nodiscard]] std::vector<DecomposedQueryPlan> getDecomposedQueryPlans(const DistributedQueryId& queryId, const Host& worker) const;
    [[nodiscard]] std::optional<DecomposedQueryPlan> getDecomposedQueryPlan(const DistributedQueryId& queryId, DecomposedQueryId id) const;

    /// Adds a plan or replaces the plan with the same id.
    void addOrUpdateDecomposedQueryPlan(const DistributedQueryId& queryId, DecomposedQueryPlan plan);
    void removeDecomposedQueryPlan(const DistributedQueryId& queryId, DecomposedQueryId id);
    void setState(const DistributedQueryId& queryId, DecomposedQueryId id, DecomposedQueryPlanState state);
    void setLocalQueryId(const DistributedQueryId& queryId, DecomposedQueryId id, LocalQueryId localQueryId);

    /// Returns the plan hosting the given operator of the global plan.
    [[nodiscard]] std::optional<DecomposedQueryPlan>
    findDecomposedQueryPlanHosting(const DistributedQueryId& queryId, OperatorId operatorId) const;

    [[nodiscard]] std::vector<PlacedNetworkChannel> getChannels(const DistributedQueryId& queryId) const;
    /// Returns the channels from `upstreamWorker` to `downstreamWorker`, i.e., the channels using this topology link.
    [[nodiscard]] std::vector<PlacedNetworkChannel>
    getChannelsOnLink(const DistributedQueryId& queryId, const Host& upstreamWorker, const Host& downstreamWorker) const;
    /// Returns the hops of the channel chain realizing the logical edge upstreamOperator -> downstreamOperator, in data flow order.
    [[nodiscard]] std::vector<PlacedNetworkChannel>
    getChannelChain(const DistributedQueryId& queryId, OperatorId upstreamOperator, OperatorId downstreamOperator) const;
    void addChannel(const DistributedQueryId& queryId, PlacedNetworkChannel channel);
    void removeChannel(const DistributedQueryId& queryId, const std::string& channelId);

    /// Returns the queries with at least one decomposed query plan on the worker (the original's `getPlacedSharedQueryIds`).
    [[nodiscard]] std::set<DistributedQueryId> getPlacedQueryIds(const Host& worker) const;

    [[nodiscard]] DecomposedQueryId getNextDecomposedQueryId();
    [[nodiscard]] DecomposedQueryPlanVersion getNextVersion();

private:
    struct QueryEntry
    {
        std::map<DecomposedQueryId, DecomposedQueryPlan> plans;
        std::vector<PlacedNetworkChannel> channels;
    };

    const QueryEntry& getEntry(const DistributedQueryId& queryId) const;
    QueryEntry& getEntry(const DistributedQueryId& queryId);

    mutable std::mutex mutex;
    std::unordered_map<DistributedQueryId, QueryEntry> queries;
    std::atomic<uint64_t> nextDecomposedQueryId = DecomposedQueryId::INITIAL;
    std::atomic<uint64_t> nextVersion = DecomposedQueryPlanVersion::INITIAL;
};

std::ostream& operator<<(std::ostream& os, DecomposedQueryPlanState state);

}

FMT_OSTREAM(NES::DecomposedQueryPlanState);
