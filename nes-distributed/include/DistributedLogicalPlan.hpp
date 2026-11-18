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

#include <cstddef>
#include <functional>
#include <numeric>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <fmt/format.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A network channel that the query decomposition inserted between a network sink and a network source.
/// A logical edge (upstream operator -> downstream operator) whose operators are placed on different workers is realized by a chain of
/// channels, one per hop of the topology path. Workers between the two operators run forwarding plans (network source -> network sink).
/// The upstream and downstream operator ids correspond to the original's UPSTREAM_LOGICAL_OPERATOR_ID and DOWNSTREAM_LOGICAL_OPERATOR_ID
/// properties of the network operators; they are the same for all hops of the chain.
/// The network operators of a channel are identified by the channel id in their descriptor configuration.
struct NetworkChannel
{
    std::string id;
    Host upstreamWorker;
    Host downstreamWorker;
    OperatorId upstreamOperatorId{INVALID_OPERATOR_ID};
    OperatorId downstreamOperatorId{INVALID_OPERATOR_ID};
    /// Index of the local plan holding the network sink among the local plans of the upstream worker.
    size_t upstreamPlanIndex = 0;
    /// Index of the local plan holding the network source among the local plans of the downstream worker.
    size_t downstreamPlanIndex = 0;

    bool operator==(const NetworkChannel&) const = default;
};

class DistributedLogicalPlan
{
public:
    DistributedLogicalPlan(std::unordered_map<Host, std::vector<LogicalPlan>> localPlans, LogicalPlan globalPlan)
        : queryId(globalPlan.getQueryId().getDistributedQueryId()), localPlans(std::move(localPlans)), globalPlan(std::move(globalPlan))
    {
        PRECONDITION(not this->localPlans.empty(), "Input plan should not be empty");
    }

    /// `hostedOperators` lists for every local plan (same order as `localPlans`) the ids of the operators of the global plan it hosts.
    /// Logical operators get a new id whenever they are modified, so the operators of the local plans have other ids than the operators
    /// of the global plan.
    DistributedLogicalPlan(
        std::unordered_map<Host, std::vector<LogicalPlan>> localPlans,
        LogicalPlan globalPlan,
        std::vector<NetworkChannel> channels,
        std::unordered_map<Host, std::vector<std::vector<OperatorId>>> hostedOperators)
        : DistributedLogicalPlan(std::move(localPlans), std::move(globalPlan))
    {
        this->channels = std::move(channels);
        this->hostedOperators = std::move(hostedOperators);
    }

    /// Subscript operator for accessing plans by worker id
    const std::vector<LogicalPlan>& operator[](const Host& worker) const
    {
        if (const auto it = localPlans.find(worker); it != localPlans.end())
        {
            return it->second;
        }
        throw std::out_of_range(fmt::format("No plan found in decomposed plan under worker {}", worker));
    }

    std::vector<LogicalPlan>& operator[](const Host& worker) { return localPlans.at(worker); }

    [[nodiscard]] size_t size() const
    {
        return std::ranges::fold_left(
            localPlans | std::views::values | std::views::transform(&std::vector<LogicalPlan>::size), 0, std::plus{});
    }

    [[nodiscard]] const LogicalPlan& getGlobalPlan() const { return globalPlan; }

    /// The network channels between the local plans, in the order in which the decomposition created them. The hops of a chain are
    /// ordered from upstream to downstream.
    [[nodiscard]] const std::vector<NetworkChannel>& getChannels() const { return channels; }

    /// The ids of the operators of the global plan hosted by the local plans of the worker, in the order of the local plans.
    /// Empty if the plan was not created by the query decomposition.
    [[nodiscard]] std::vector<std::vector<OperatorId>> getHostedOperators(const Host& worker) const
    {
        if (const auto it = hostedOperators.find(worker); it != hostedOperators.end())
        {
            return it->second;
        }
        return {};
    }

    [[nodiscard]] const DistributedQueryId& getQueryId() const { return queryId; }

    void setQueryId(DistributedQueryId queryId) { this->queryId = std::move(queryId); }

    [[nodiscard]] auto begin() const { return localPlans.begin(); }

    [[nodiscard]] auto end() const { return localPlans.end(); }

private:
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
    LogicalPlan globalPlan;
    std::vector<NetworkChannel> channels;
    std::unordered_map<Host, std::vector<std::vector<OperatorId>>> hostedOperators;
};
}
