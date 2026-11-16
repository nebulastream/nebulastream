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

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Catalog.hpp>
#include <CoordinatorStatement.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

/// The part of a query that runs on one worker.
struct PlannedFragment
{
    Host host;
    LogicalPlan plan;
    /// Counts against the operator capacity of the worker.
    /// Sources and sinks are not counted.
    int32_t numOperators;
    bool hasSource;
};

/// What the planner returns for one SQL statement.
/// The statement is sent to the coordinator as JSON.
/// The fragments are only filled for a query.
struct PlanOutput
{
    CoordinatorStatement statement;
    std::vector<PlannedFragment> fragments;
};

/// Turns one SQL statement into the statement that the coordinator executes.
/// A query is also optimized and split into one plan per worker.
class SqlPlanner
{
public:
    explicit SqlPlanner(
        const std::shared_ptr<Catalog>& catalog, QueryOptimizerConfiguration optimizerConfig, std::optional<Host> defaultHost);

    [[nodiscard]] PlanOutput plan(std::string_view sql) const;

private:
    StatementBinder binder;
    QueryOptimizer optimizer;
    std::optional<Host> defaultHost;
};

}
