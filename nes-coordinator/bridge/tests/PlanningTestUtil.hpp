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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <nes-coordinator-bridge/planner.h>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <CoordinatorStatement.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SnapshotCatalog.hpp>
#include <SqlPlanner.hpp>

namespace NES
{
/// Plans one statement against the snapshot, without a default host.
inline PlanOutput planWith(const Bridge::CatalogSnapshot& snapshot, const std::string_view sql)
{
    return SqlPlanner{
        std::make_shared<SnapshotCatalog>(snapshot, std::nullopt), defaultConfiguration<QueryOptimizerConfiguration>(), std::nullopt}
        .plan(sql);
}

/// A catalog snapshot that was filled from SQL, and the query to plan against it.
struct PlanningScenario
{
    Bridge::CatalogSnapshot snapshot;
    std::string query;

    /// Plans the query and returns the plans of each worker.
    /// A planning failure is thrown.
    [[nodiscard]] std::unordered_map<Host, std::vector<LogicalPlan>> optimize() const
    {
        auto fragments = planWith(snapshot, query).fragments;
        std::unordered_map<Host, std::vector<LogicalPlan>> plansByHost;
        for (auto& fragment : fragments)
        {
            plansByHost[fragment.host].push_back(std::move(fragment.plan));
        }
        return plansByHost;
    }
};

/// Plans every statement of `setupSql` and adds what it creates to a snapshot, which is paired with `query`.
/// The setup may create workers, logical and physical sources, and sinks.
/// The snapshot holds the planned JSON, as the coordinator's catalog does.
inline PlanningScenario loadAndBind(const std::string_view setupSql, const std::string_view query)
{
    const auto statements = AntlrSQLQueryParser::splitStatements(setupSql);
    if (!statements.has_value())
    {
        throw Exception{statements.error()};
    }
    Bridge::CatalogSnapshot snapshot{};
    snapshot.next_source_id = 1;
    snapshot.next_sink_id = 1;
    for (const auto& statement : *statements)
    {
        const auto planned = planWith(snapshot, statement).statement;
        const auto& variant = planned.variant();
        if (rfl::holds_alternative<PlannedCreateWorker>(variant))
        {
            const auto& worker = rfl::get<PlannedCreateWorker>(variant);
            snapshot.workers.push_back(
                {.host_addr = worker.hostAddr.value(),
                 .data_addr = worker.dataAddr.value(),
                 .max_operators = worker.maxOperators.value().value_or(-1)});
            for (const auto& peer : worker.peers)
            {
                snapshot.links.push_back({.src_addr = worker.hostAddr.value(), .dst_addr = peer});
            }
        }
        else if (rfl::holds_alternative<PlannedCreateLogicalSource>(variant))
        {
            const auto& source = rfl::get<PlannedCreateLogicalSource>(variant);
            snapshot.logical_sources.push_back({.name = source.name, .schema_json = rfl::json::write(source.schema)});
        }
        else if (rfl::holds_alternative<PlannedCreatePhysicalSource>(variant))
        {
            const auto& source = rfl::get<PlannedCreatePhysicalSource>(variant);
            snapshot.physical_sources.push_back({
                .id = snapshot.next_source_id++,
                .logical_source = source.logicalSourceName.value(),
                .host_addr = source.host.value(),
                .source_type = source.sourceType.value(),
                .source_config_json = rfl::json::write(source.sourceConfig.value()),
                .parser_config_json = rfl::json::write(source.parserConfig.value()),
                .internal = false,
            });
        }
        else if (rfl::holds_alternative<PlannedCreateSink>(variant))
        {
            const auto& sink = rfl::get<PlannedCreateSink>(variant);
            snapshot.sinks.push_back({
                .id = snapshot.next_sink_id++,
                .name = sink.name,
                .host_addr = sink.host.value(),
                .sink_type = sink.sinkType.value(),
                .schema_json = rfl::json::write(sink.schema),
                .config_json = rfl::json::write(sink.config),
                .internal = false,
            });
        }
        else
        {
            throw TestException("the planning test setup does not support the statement '{}'", statement);
        }
    }
    return PlanningScenario{.snapshot = std::move(snapshot), .query = std::string{query}};
}
}
