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
#include <unordered_map>
#include <utility>
#include <vector>
#include <DistributedQuery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

class DistributedLogicalPlan
{
public:
    DistributedLogicalPlan(std::unordered_map<Host, std::vector<LogicalPlan>> localPlans, LogicalPlan globalPlan)
        : queryId(globalPlan.getQueryId().getDistributedQueryId())
        , localPlans(std::move(localPlans))
        , globalPlan(std::move(globalPlan))
    {
        PRECONDITION(not this->localPlans.empty(), "Input plan should not be empty");
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

    [[nodiscard]] const DistributedQueryId& getQueryId() const { return queryId; }

    void setQueryId(DistributedQueryId queryId) { this->queryId = std::move(queryId); }

    [[nodiscard]] auto begin() const { return localPlans.begin(); }

    [[nodiscard]] auto end() const { return localPlans.end(); }

private:
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
    LogicalPlan globalPlan;
};
}
