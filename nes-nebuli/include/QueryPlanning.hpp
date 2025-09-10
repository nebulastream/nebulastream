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
#include <cstddef>
#include <functional>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bits/ranges_algo.h>
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

    ~OptimizedLogicalPlan() = default;
    OptimizedLogicalPlan(OptimizedLogicalPlan&&) = default;
    OptimizedLogicalPlan(const OptimizedLogicalPlan&) = default;
    OptimizedLogicalPlan& operator=(OptimizedLogicalPlan&&) = default;
    OptimizedLogicalPlan& operator=(const OptimizedLogicalPlan&) = default;
};

struct PlacedLogicalPlan
{
    LogicalPlan plan;

    explicit PlacedLogicalPlan(LogicalPlan placedPlan) : plan{std::move(placedPlan)} { }

    ~PlacedLogicalPlan() = default;
    PlacedLogicalPlan(PlacedLogicalPlan&&) = default;
    PlacedLogicalPlan(const PlacedLogicalPlan&) = default;
    PlacedLogicalPlan& operator=(PlacedLogicalPlan&&) = default;
    PlacedLogicalPlan& operator=(const PlacedLogicalPlan&) = default;
};

struct DecomposedLogicalPlan
{
    std::unordered_map<GrpcAddr, std::vector<LogicalPlan>> localPlans;

    DecomposedLogicalPlan() = default;

    explicit DecomposedLogicalPlan(std::unordered_map<GrpcAddr, std::vector<LogicalPlan>> plans) : localPlans{std::move(plans)} { }

    ~DecomposedLogicalPlan() = default;
    DecomposedLogicalPlan(DecomposedLogicalPlan&&) = default;
    DecomposedLogicalPlan(const DecomposedLogicalPlan&) = default;
    DecomposedLogicalPlan& operator=(DecomposedLogicalPlan&&) = default;
    DecomposedLogicalPlan& operator=(const DecomposedLogicalPlan&) = default;
};

struct DistributedLogicalPlan
{
    using CapacityCleanup = std::function<void(const DistributedLogicalPlan&)>;

    DistributedLogicalPlan() = default;

    explicit DistributedLogicalPlan(DecomposedLogicalPlan&& plan, CapacityCleanup cleanupCallback = {})
        : plan(std::move(plan)), cleanupCallback(std::move(cleanupCallback))
    {
        PRECONDITION(not this->plan.localPlans.empty(), "Input plan should not be empty");
    }

    ~DistributedLogicalPlan()
    {
        if (cleanupCallback)
        {
            cleanupCallback(*this);
        }
    }

    const LogicalPlan& operator*() const { return plan.localPlans.begin()->second.front(); }

    const LogicalPlan& operator*() { return plan.localPlans.begin()->second.front(); }

    const LogicalPlan* operator->() const { return &(**this); }

    /// Subscript operator for accessing plans by its grpc addr
    const std::vector<LogicalPlan>& operator[](const GrpcAddr& grpc) const { return plan.localPlans.at(grpc); }

    std::vector<LogicalPlan>& operator[](const GrpcAddr& grpc) { return plan.localPlans.at(grpc); }

    size_t size() const
    {
        return std::ranges::fold_left(
            plan.localPlans | std::views::values | std::views::transform(&std::vector<LogicalPlan>::size), 0, std::plus{});
    }

    auto view() const { return plan; }

    auto begin() const { return plan.localPlans.cbegin(); }

    auto end() const { return plan.localPlans.cend(); }

    /// Move constructor and assignment
    DistributedLogicalPlan(DistributedLogicalPlan&& other) noexcept = default;
    DistributedLogicalPlan& operator=(DistributedLogicalPlan&& other) noexcept = default;

    /// Copy constructor - copies plan data but not the cleanup callback
    /// This ensures cleanup only happens once (for the original/moved instance)
    DistributedLogicalPlan(const DistributedLogicalPlan& other) : plan(other.plan)
    {
        /// Note: cleanup callback is intentionally not copied
        /// Cleanup should only happen once for the original instance
    }

    /// Copy assignment - copies plan data but not the cleanup callback
    DistributedLogicalPlan& operator=(const DistributedLogicalPlan& other)
    {
        if (this != &other)
        {
            plan = other.plan;
            /// Note: cleanup callback is intentionally not copied
        }
        return *this;
    }

private:
    DecomposedLogicalPlan plan;
    CapacityCleanup cleanupCallback;
};
}

struct QueryPlanningContext
{
    LocalQueryId id;
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
[[nodiscard]] std::vector<T> getOperatorByType(const PlanStage::DistributedLogicalPlan& distributedPlan)
{
    return distributedPlan | std::views::values | std::views::join
        | std::views::transform([](const auto& localPlan) { return getOperatorByType<T>(localPlan); }) | std::views::join
        | std::ranges::to<std::vector>();
}

}
