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

#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>

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
};
}

struct QueryPlanningContext
{
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;
};

class QueryPlanner
{
    QueryPlanningContext context;

    explicit QueryPlanner(QueryPlanningContext context) : context(std::move(context)) { }

public:
    QueryPlanner() = delete;

    static QueryPlanner with(QueryPlanningContext context) { return QueryPlanner{std::move(context)}; }

    PlanStage::OptimizedLogicalPlan plan(PlanStage::BoundLogicalPlan&& boundPlan) &&;
};

}
