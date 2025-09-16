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

#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>

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
}

struct QueryPlanningContext
{
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;
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

    PlanStage::OptimizedLogicalPlan plan(PlanStage::BoundLogicalPlan&& boundPlan) &&;
};

}
