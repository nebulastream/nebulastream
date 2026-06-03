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

#include <DistributedLogicalPlan.hpp>

#include <cstddef>
#include <functional>
#include <numeric>
#include <ranges>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <coro/coro.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{


DistributedLogicalPlan::DistributedLogicalPlan(std::unordered_map<Host, std::vector<LogicalPlan>> localPlans, LogicalPlan globalPlan)
    : queryId(globalPlan.getQueryId().getDistributedQueryId()), localPlans(std::move(localPlans)), globalPlan(std::move(globalPlan))
{
    PRECONDITION(not this->localPlans.empty(), "Input plan should not be empty");
}

const std::vector<LogicalPlan>& DistributedLogicalPlan::operator[](const Host& worker) const
{
    if (const auto it = localPlans.find(worker); it != localPlans.end())
    {
        return it->second;
    }
    throw std::out_of_range(fmt::format("No plan found in decomposed plan under worker {}", worker));
}

std::vector<LogicalPlan>& DistributedLogicalPlan::operator[](const Host& worker)
{
    return localPlans.at(worker);
}

size_t DistributedLogicalPlan::size() const
{
    return std::ranges::fold_left(localPlans | std::views::values | std::views::transform(&std::vector<LogicalPlan>::size), 0, std::plus{});
}

const LogicalPlan& DistributedLogicalPlan::getGlobalPlan() const
{
    return globalPlan;
}

const DistributedQueryId& DistributedLogicalPlan::getQueryId() const
{
    return queryId;
}

void DistributedLogicalPlan::setQueryId(DistributedQueryId queryId)
{
    this->queryId = std::move(queryId);
}

coro::generator<std::vector<std::pair<Host, LogicalPlan>>> topologicalSort(std::unordered_map<Host, std::vector<LogicalPlan>> localPlans)
{
    std::vector<std::pair<Host, LogicalPlan>> plansByNode;
    for (const auto& [host, plans] : localPlans)
    {
        for (const auto& plan : plans)
        {
            plansByNode.emplace_back(host, plan);
        }
    }

    std::unordered_set<std::string> usedChannels;

    const auto isReady = [&](const auto& hostAndPlan)
    {
        const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(hostAndPlan.second);
        for (const auto& source : sources)
        {
            if (source->getSourceDescriptor().getSourceType() != "NETWORK")
            {
                continue;
            }
            const auto& config = source->getSourceDescriptor().getConfig();
            const auto channel = config.find("CHANNEL");
            if (channel == config.end() || !usedChannels.contains(std::get<std::string>(channel->second)))
            {
                return false;
            }
        }
        return true;
    };

    while (!plansByNode.empty())
    {
        /// Move all plans whose network inputs are satisfied to the front; the suffix `[mid, end)` is still waiting.
        const auto mid = std::ranges::partition(plansByNode, isReady).begin();
        if (mid == plansByNode.begin())
        {
            /// No progress this round — remaining plans depend on channels that will never be produced.
            co_return;
        }

        std::vector<std::pair<Host, LogicalPlan>> ready(std::make_move_iterator(plansByNode.begin()), std::make_move_iterator(mid));
        plansByNode.erase(plansByNode.begin(), mid);

        for (const auto& [_, plan] : ready)
        {
            for (const auto& sink : getOperatorByType<SinkLogicalOperator>(plan))
            {
                if (sink->getSinkDescriptor()->getSinkType() == "NETWORK")
                {
                    const auto& config = sink->getSinkDescriptor()->getConfig();
                    usedChannels.insert(std::get<std::string>(config.at("CHANNEL")));
                }
            }
        }

        co_yield std::move(ready);
    }
}
}
