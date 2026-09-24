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

#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <CoordinatorStatement.hpp>
#include <ErrorHandling.hpp>
#include <HostPolicy.hpp>
#include <QueryOptimizer.hpp>

namespace NES
{

/// One fragment of a placed query, with what the coordinator records about it.
struct PlannedFragment
{
    Host host;
    LogicalPlan plan;
    /// The count charged against the worker's operator capacity, which excludes the fragment's sources and sinks.
    int32_t numOperators;
    bool hasSource;
};

/// The planner's answer to one statement.
/// The statement serializes to the JSON that the coordinator's own statement deserializes from.
/// The fragments and the source and sink ids are filled only for a query.
/// Their per-worker plans are sent out of band as protobuf bytes rather than inside the JSON.
struct PlanOutput
{
    CoordinatorStatement statement;
    std::vector<PlannedFragment> fragments;
    std::vector<PhysicalSourceId> sourceIds;
    std::vector<SinkId> sinkIds;
};

class SqlPlanner
{
public:
    explicit SqlPlanner(const std::shared_ptr<Catalog>& catalog, QueryOptimizerConfiguration optimizerConfig, HostPolicy hostPolicy);

    [[nodiscard]] std::expected<PlanOutput, Exception> plan(std::string_view sql) const;

private:
    StatementBinder binder;
    QueryOptimizer optimizer;
    HostPolicy hostPolicy;
};

}
