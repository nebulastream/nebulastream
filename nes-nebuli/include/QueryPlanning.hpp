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

#include <algorithm>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DistributedQuery.hpp>

#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

namespace PlanStage
{
struct BoundLogicalPlan
{
    LogicalPlan plan;
};

struct OptimizedLogicalPlan
{
    LogicalPlan plan;

    explicit OptimizedLogicalPlan(LogicalPlan optimizedPlan) : plan{std::move(optimizedPlan)} { }

    OptimizedLogicalPlan(OptimizedLogicalPlan&&) = default;
    OptimizedLogicalPlan(const OptimizedLogicalPlan&) = default;
    OptimizedLogicalPlan& operator=(OptimizedLogicalPlan&&) = default;
    OptimizedLogicalPlan& operator=(const OptimizedLogicalPlan&) = default;
};

struct PlacedLogicalPlan
{
    LogicalPlan plan;

    explicit PlacedLogicalPlan(LogicalPlan placedPlan) : plan{std::move(placedPlan)} { }

    PlacedLogicalPlan(PlacedLogicalPlan&&) = default;
    PlacedLogicalPlan(const PlacedLogicalPlan&) = default;
    PlacedLogicalPlan& operator=(PlacedLogicalPlan&&) = default;
    PlacedLogicalPlan& operator=(const PlacedLogicalPlan&) = default;
};

template <typename AddressType>
struct DecomposedLogicalPlan
{
    std::unordered_map<AddressType, std::vector<LogicalPlan>> localPlans;

    explicit DecomposedLogicalPlan(std::unordered_map<AddressType, std::vector<LogicalPlan>> plans) : localPlans{std::move(plans)} { }

    DecomposedLogicalPlan(DecomposedLogicalPlan&&) = default;
    DecomposedLogicalPlan(const DecomposedLogicalPlan&) = default;
    DecomposedLogicalPlan& operator=(DecomposedLogicalPlan&&) = default;
    DecomposedLogicalPlan& operator=(const DecomposedLogicalPlan&) = default;
};

struct DistributedLogicalPlan
{
    DistributedLogicalPlan(DecomposedLogicalPlan<GrpcAddr>&& plan, OptimizedLogicalPlan globalPlan)
        : plan(std::move(plan)), globalPlan(std::move(globalPlan))
    {
        PRECONDITION(not this->plan.localPlans.empty(), "Input plan should not be empty");
    }

    const LogicalPlan& operator*() const { return plan.localPlans.begin()->second.front(); }

    const LogicalPlan& operator*() { return plan.localPlans.begin()->second.front(); }

    const LogicalPlan* operator->() const { return &(**this); }

    /// Subscript operator for accessing plans by its grpc addr
    const std::vector<LogicalPlan>& operator[](const GrpcAddr& grpc) const
    {
        const auto it = plan.localPlans.find(grpc);
        if (it == plan.localPlans.end())
        {
            throw std::out_of_range(fmt::format("No plan found in decomposed plan under addr {}", grpc));
        }
        return it->second;
    }

    std::vector<LogicalPlan>& operator[](const GrpcAddr& connection) { return plan.localPlans.at(connection); }

    size_t size() const
    {
        return std::ranges::fold_left(
            plan.localPlans | std::views::values | std::views::transform(&std::vector<LogicalPlan>::size), 0, std::plus{});
    }

    auto view() const { return plan; }

    const LogicalPlan& getGlobalPlan() const { return globalPlan.plan; }

    auto begin() const { return plan.localPlans.cbegin(); }

    auto end() const { return plan.localPlans.cend(); }

    [[nodiscard]] const DistributedQueryId& getQueryId() const { return queryId; }

    void setQueryId(DistributedQueryId query_id) { queryId = std::move(query_id); }

private:
    DistributedQueryId queryId = DistributedQueryId(DistributedQueryId::INVALID.value);
    DecomposedLogicalPlan<GrpcAddr> plan;
    OptimizedLogicalPlan globalPlan;
};
}

struct QueryPlanningContext
{
    std::string sqlString;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;
    SharedPtr<WorkerCatalog> workerCatalog;
};

class QueryPlanner
{
    QueryPlanningContext& context;

    explicit QueryPlanner(QueryPlanningContext& context) : context(context) { }

public:
    QueryPlanner() = delete;
    ~QueryPlanner() = default;
    QueryPlanner(const QueryPlanner&) = delete;
    QueryPlanner& operator=(const QueryPlanner&) = delete;
    QueryPlanner(QueryPlanner&&) = delete;
    QueryPlanner& operator=(QueryPlanner&&) = delete;

    static QueryPlanner with(QueryPlanningContext& context) { return QueryPlanner{context}; }

    PlanStage::DistributedLogicalPlan plan(PlanStage::BoundLogicalPlan&& boundPlan) &&;
};

template <class T>
[[nodiscard]] std::vector<TypedLogicalOperator<T>> getOperatorByType(const PlanStage::DistributedLogicalPlan& distributedPlan)
{
    return distributedPlan | std::views::values | std::views::join
        | std::views::transform([](const auto& localPlan) { return getOperatorByType<T>(localPlan); }) | std::views::join
        | std::ranges::to<std::vector>();
}

}
