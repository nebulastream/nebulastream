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

#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <coro/coro.hpp>

namespace NES
{

class DistributedLogicalPlan
{
public:
    DistributedLogicalPlan(std::unordered_map<Host, std::vector<LogicalPlan>> localPlans, LogicalPlan globalPlan);

    /// Subscript operator for accessing plans by worker id
    const std::vector<LogicalPlan>& operator[](const Host& worker) const;

    std::vector<LogicalPlan>& operator[](const Host& worker);

    [[nodiscard]] size_t size() const;

    [[nodiscard]] const LogicalPlan& getGlobalPlan() const;

    [[nodiscard]] const DistributedQueryId& getQueryId() const;

    void setQueryId(DistributedQueryId queryId);

    [[nodiscard]] auto begin() const { return localPlans.begin(); }

    [[nodiscard]] auto end() const { return localPlans.end(); }

    [[nodiscard]] auto begin() { return localPlans.begin(); }

    [[nodiscard]] auto end() { return localPlans.end(); }

    bool operator==(const DistributedLogicalPlan&) const = default;

private:
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
    LogicalPlan globalPlan;
};

/// Generator that provides a topological sort of query plans. Each vector represents a set of query plans that on query plans on previous
/// iterations.
coro::generator<std::vector<std::pair<Host, LogicalPlan>>> topologicalSort(std::unordered_map<Host, std::vector<LogicalPlan>> localPlans);

}
