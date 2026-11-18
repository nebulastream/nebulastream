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

#include <GlobalExecutionPlan.hpp>

#include <algorithm>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> ///NOLINT(misc-include-cleaner)
#include <magic_enum/magic_enum.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{

std::unordered_map<Host, std::vector<DecomposedQueryId>>
GlobalExecutionPlan::addQuery(const DistributedQueryId& queryId, const DistributedLogicalPlan& plan)
{
    const std::scoped_lock lock(mutex);
    PRECONDITION(not queries.contains(queryId), "Query {} is already part of the global execution plan", queryId);
    const auto version = DecomposedQueryPlanVersion(nextVersion++);

    QueryEntry entry;
    std::unordered_map<Host, std::vector<DecomposedQueryId>> planIds;
    for (const auto& [worker, localPlans] : plan)
    {
        const auto hostedOperators = plan.getHostedOperators(worker);
        for (size_t index = 0; index < localPlans.size(); ++index)
        {
            const auto id = DecomposedQueryId(nextDecomposedQueryId++);
            planIds[worker].emplace_back(id);
            entry.plans.emplace(
                id,
                DecomposedQueryPlan{
                    .id = id,
                    .version = version,
                    .worker = worker,
                    .plan = localPlans.at(index),
                    .hostedOperators = index < hostedOperators.size()
                        ? std::set(hostedOperators[index].begin(), hostedOperators[index].end())
                        : std::set<OperatorId>{},
                    .state = DecomposedQueryPlanState::Running,
                    .localQueryId = std::nullopt});
        }
    }
    for (const auto& channel : plan.getChannels())
    {
        entry.channels.emplace_back(
            PlacedNetworkChannel{
                .channel = channel,
                .upstreamPlan = planIds.at(channel.upstreamWorker).at(channel.upstreamPlanIndex),
                .downstreamPlan = planIds.at(channel.downstreamWorker).at(channel.downstreamPlanIndex)});
    }
    queries.emplace(queryId, std::move(entry));
    return planIds;
}

void GlobalExecutionPlan::removeQuery(const DistributedQueryId& queryId)
{
    const std::scoped_lock lock(mutex);
    queries.erase(queryId);
}

bool GlobalExecutionPlan::contains(const DistributedQueryId& queryId) const
{
    const std::scoped_lock lock(mutex);
    return queries.contains(queryId);
}

const GlobalExecutionPlan::QueryEntry& GlobalExecutionPlan::getEntry(const DistributedQueryId& queryId) const
{
    const auto it = queries.find(queryId);
    PRECONDITION(it != queries.end(), "Query {} is not part of the global execution plan", queryId);
    return it->second;
}

GlobalExecutionPlan::QueryEntry& GlobalExecutionPlan::getEntry(const DistributedQueryId& queryId)
{
    const auto it = queries.find(queryId);
    PRECONDITION(it != queries.end(), "Query {} is not part of the global execution plan", queryId);
    return it->second;
}

std::vector<DecomposedQueryPlan> GlobalExecutionPlan::getDecomposedQueryPlans(const DistributedQueryId& queryId) const
{
    const std::scoped_lock lock(mutex);
    return getEntry(queryId).plans | std::views::values | std::ranges::to<std::vector>();
}

std::vector<DecomposedQueryPlan> GlobalExecutionPlan::getDecomposedQueryPlans(const DistributedQueryId& queryId, const Host& worker) const
{
    const std::scoped_lock lock(mutex);
    return getEntry(queryId).plans | std::views::values | std::views::filter([&worker](const auto& plan) { return plan.worker == worker; })
        | std::ranges::to<std::vector>();
}

std::optional<DecomposedQueryPlan>
GlobalExecutionPlan::getDecomposedQueryPlan(const DistributedQueryId& queryId, const DecomposedQueryId id) const
{
    const std::scoped_lock lock(mutex);
    const auto& plans = getEntry(queryId).plans;
    if (const auto it = plans.find(id); it != plans.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void GlobalExecutionPlan::addOrUpdateDecomposedQueryPlan(const DistributedQueryId& queryId, DecomposedQueryPlan plan)
{
    const std::scoped_lock lock(mutex);
    const auto id = plan.id;
    getEntry(queryId).plans.insert_or_assign(id, std::move(plan));
}

void GlobalExecutionPlan::removeDecomposedQueryPlan(const DistributedQueryId& queryId, const DecomposedQueryId id)
{
    const std::scoped_lock lock(mutex);
    getEntry(queryId).plans.erase(id);
}

void GlobalExecutionPlan::setState(const DistributedQueryId& queryId, const DecomposedQueryId id, const DecomposedQueryPlanState state)
{
    const std::scoped_lock lock(mutex);
    auto& plans = getEntry(queryId).plans;
    const auto it = plans.find(id);
    PRECONDITION(it != plans.end(), "Decomposed query plan {} is not part of query {}", id, queryId);
    it->second.state = state;
}

void GlobalExecutionPlan::setLocalQueryId(const DistributedQueryId& queryId, const DecomposedQueryId id, LocalQueryId localQueryId)
{
    const std::scoped_lock lock(mutex);
    auto& plans = getEntry(queryId).plans;
    const auto it = plans.find(id);
    PRECONDITION(it != plans.end(), "Decomposed query plan {} is not part of query {}", id, queryId);
    it->second.localQueryId = localQueryId;
}

std::optional<DecomposedQueryPlan>
GlobalExecutionPlan::findDecomposedQueryPlanHosting(const DistributedQueryId& queryId, const OperatorId operatorId) const
{
    const std::scoped_lock lock(mutex);
    for (const auto& plan : getEntry(queryId).plans | std::views::values)
    {
        if (plan.hostedOperators.contains(operatorId))
        {
            return plan;
        }
    }
    return std::nullopt;
}

std::vector<PlacedNetworkChannel> GlobalExecutionPlan::getChannels(const DistributedQueryId& queryId) const
{
    const std::scoped_lock lock(mutex);
    return getEntry(queryId).channels;
}

std::vector<PlacedNetworkChannel>
GlobalExecutionPlan::getChannelsOnLink(const DistributedQueryId& queryId, const Host& upstreamWorker, const Host& downstreamWorker) const
{
    const std::scoped_lock lock(mutex);
    return getEntry(queryId).channels
        | std::views::filter(
               [&](const auto& placed)
               { return placed.channel.upstreamWorker == upstreamWorker && placed.channel.downstreamWorker == downstreamWorker; })
        | std::ranges::to<std::vector>();
}

std::vector<PlacedNetworkChannel> GlobalExecutionPlan::getChannelChain(
    const DistributedQueryId& queryId, const OperatorId upstreamOperator, const OperatorId downstreamOperator) const
{
    const std::scoped_lock lock(mutex);
    auto hops
        = getEntry(queryId).channels
        | std::views::filter(
              [&](const auto& placed)
              {
                  return placed.channel.upstreamOperatorId == upstreamOperator && placed.channel.downstreamOperatorId == downstreamOperator;
              })
        | std::ranges::to<std::vector>();
    /// Order the hops: the first hop is the one whose upstream plan is not the downstream plan of another hop.
    std::vector<PlacedNetworkChannel> chain;
    chain.reserve(hops.size());
    while (not hops.empty())
    {
        const auto next = std::ranges::find_if(
            hops,
            [&](const auto& hop)
            {
                return chain.empty()
                    ? std::ranges::none_of(hops, [&](const auto& other) { return other.downstreamPlan == hop.upstreamPlan; })
                    : hop.upstreamPlan == chain.back().downstreamPlan;
            });
        INVARIANT(
            next != hops.end(),
            "The channels from {} to {} of query {} do not form a chain",
            upstreamOperator,
            downstreamOperator,
            queryId);
        chain.emplace_back(*next);
        hops.erase(next);
    }
    return chain;
}

void GlobalExecutionPlan::addChannel(const DistributedQueryId& queryId, PlacedNetworkChannel channel)
{
    const std::scoped_lock lock(mutex);
    getEntry(queryId).channels.emplace_back(std::move(channel));
}

void GlobalExecutionPlan::removeChannel(const DistributedQueryId& queryId, const std::string& channelId)
{
    const std::scoped_lock lock(mutex);
    std::erase_if(getEntry(queryId).channels, [&channelId](const auto& placed) { return placed.channel.id == channelId; });
}

std::set<DistributedQueryId> GlobalExecutionPlan::getPlacedQueryIds(const Host& worker) const
{
    const std::scoped_lock lock(mutex);
    std::set<DistributedQueryId> placed;
    for (const auto& [queryId, entry] : queries)
    {
        if (std::ranges::any_of(entry.plans | std::views::values, [&worker](const auto& plan) { return plan.worker == worker; }))
        {
            placed.insert(queryId);
        }
    }
    return placed;
}

DecomposedQueryId GlobalExecutionPlan::getNextDecomposedQueryId()
{
    return DecomposedQueryId(nextDecomposedQueryId++);
}

DecomposedQueryPlanVersion GlobalExecutionPlan::getNextVersion()
{
    return DecomposedQueryPlanVersion(nextVersion++);
}

std::ostream& operator<<(std::ostream& os, const DecomposedQueryPlanState state)
{
    return os << magic_enum::enum_name(state);
}

}
