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

#include <SqlPlanner.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <Catalog.hpp>
#include <CoordinatorStatement.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>

#include <CatalogConfig.hpp>
#include <HostPolicy.hpp>
#include <Model.hpp>

namespace NES
{
namespace
{
/// An unset filter selects every query.
QueryFilters deriveQueryFilters(const QuerySelection& selection)
{
    return selection.getTarget()
        .transform(
            [](const std::variant<QueryId, Identifier>& target)
            {
                return std::visit(
                    Overloaded{
                        [](const QueryId id) { return QueryFilters{.ids = std::vector{id}, .name = std::nullopt}; },
                        [](const Identifier& name) { return QueryFilters{.ids = std::nullopt, .name = name}; }},
                    target);
            })
        .value_or(QueryFilters{.ids = std::nullopt, .name = std::nullopt});
}

/// The fragments of a placed query, and the ids of the sources and sinks that it uses.
struct QueryPlacement
{
    std::vector<PlannedFragment> fragments;
    std::vector<PhysicalSourceId> sourceIds;
    std::vector<SinkId> sinkIds;
};

QueryPlacement recordPlacement(const std::unordered_map<Host, std::vector<LogicalPlan>>& placed)
{
    QueryPlacement placement;
    for (const auto& [host, plans] : placed)
    {
        for (const auto& plan : plans)
        {
            const auto sinks = getOperatorByType<SinkLogicalOperator>(plan);
            const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(plan);
            for (const auto& source : sources)
            {
                placement.sourceIds.push_back(source->getSourceDescriptor().getPhysicalSourceId());
            }
            for (const auto& sink : sinks)
            {
                if (const auto& descriptor = sink->getSinkDescriptor(); descriptor.has_value())
                {
                    placement.sinkIds.push_back(descriptor->getSinkId());
                }
            }
            placement.fragments.push_back(
                PlannedFragment{
                    .host = host,
                    .plan = plan,
                    .numOperators = static_cast<int32_t>(flatten(plan).size() - sinks.size() - sources.size()),
                    .hasSource = !sources.empty(),
                });
        }
    }
    return placement;
}

/// The sections are always printed in the same order.
/// The plan is optimized once, so operator ids match across sections.
std::string computeExplainOutput(const ExplainQueryStatement& statement, const QueryOptimizer& optimizer)
{
    auto formatPlan = [&](const LogicalPlan& plan) -> std::string
    {
        switch (statement.explainFormat)
        {
            case ExplainFormat::Visual: {
                std::stringstream stringstream;
                auto renderer = PlanRenderer<LogicalPlan, LogicalOperator>(stringstream, ExplainVerbosity::Short);
                renderer.dump(plan);
                return stringstream.str();
            }
            case ExplainFormat::Text:
                return explain(plan, ExplainVerbosity::Short);
            case ExplainFormat::Verbose:
                return explain(plan, ExplainVerbosity::Debug);
        }
        std::unreachable();
    };

    std::stringstream explainMessage;
    const auto [globalPlan, decomposedPlans] = optimizer.optimize(statement.plan);

    if (statement.explainStages.contains(ExplainStage::Logical))
    {
        fmt::println(explainMessage, "== Initial Logical Plan ==\n{}", formatPlan(statement.plan));
    }

    if (statement.explainStages.contains(ExplainStage::Optimized))
    {
        fmt::println(explainMessage, "== Optimized Global Plan ==\n{}", formatPlan(globalPlan));
    }

    if (statement.explainStages.contains(ExplainStage::Distributed))
    {
        fmt::println(explainMessage, "== Decomposed Plans ==");
        /// The placement result is stored in an unordered map.
        /// Sort by host for deterministic output.
        auto sortedWorkerPlans = std::ranges::to<std::vector>(
            decomposedPlans | std::views::transform([](const auto& entry) { return std::addressof(entry); }));
        std::ranges::sort(sortedWorkerPlans, {}, [](const auto* entry) -> const auto& { return entry->first; });
        for (const auto* entry : sortedWorkerPlans)
        {
            const auto& [worker, plans] = *entry;
            fmt::println(explainMessage, "-- {} plan(s) on {} --", plans.size(), worker.getRawValue());
            for (const auto& [index, plan] : plans | views::enumerate)
            {
                fmt::println(explainMessage, "{}:\n{}\n", index, formatPlan(plan));
            }
        }
    }

    return explainMessage.str();
}
}

SqlPlanner::SqlPlanner(
    const std::shared_ptr<Catalog>& catalog, QueryOptimizerConfiguration optimizerConfig, std::optional<Host> defaultHost)
    : binder{[](auto* queryCtx) { return AntlrSQLQueryParser::bindLogicalQueryPlan(queryCtx); }}
    , catalog{catalog}
    , optimizer{std::move(optimizerConfig), catalog}
    , defaultHost{std::move(defaultHost)}
{
}

PlanOutput SqlPlanner::plan(const std::string_view sql) const
{
    auto boundStatement = binder.parseAndBindSingle(sql);
    if (!boundStatement)
    {
        throw std::move(boundStatement).error();
    }

    /// Only a query is placed; every other statement is a pure catalog request.
    std::optional<LogicalPlan> globalPlan;
    std::vector<PlannedFragment> fragments;

    auto statement = std::visit(
        Overloaded{
            [&](const QueryStatement& stmt) -> CoordinatorStatement
            {
                /// Placing the query writes its own sources and sinks into the catalog,
                /// so an existing query is returned before placement.
                if (stmt.ifNotExists and stmt.name.has_value() and catalog->containsQuery(*stmt.name))
                {
                    return PlannedCreateQuery{
                        .name = stmt.name, .sql = std::string{sql}, .sourceIds = {}, .sinkIds = {}, .ifNotExists = true};
                }
                auto [optimizedPlan, localPlans] = optimizer.optimize(stmt.plan);
                auto placement = recordPlacement(localPlans);
                globalPlan = std::move(optimizedPlan);
                fragments = std::move(placement.fragments);
                return PlannedCreateQuery{
                    .name = stmt.name,
                    .sql = std::string{sql},
                    .sourceIds = std::move(placement.sourceIds),
                    .sinkIds = std::move(placement.sinkIds),
                    .ifNotExists = stmt.ifNotExists};
            },
            [this](const ExplainQueryStatement& stmt) -> CoordinatorStatement
            { return PlannedExplainQuery{.explanation = computeExplainOutput(stmt, optimizer)}; },

            [](const CreateLogicalSourceStatement& stmt) -> CoordinatorStatement
            { return PlannedCreateLogicalSource{.name = stmt.name, .schema = stmt.schema, .ifNotExists = stmt.ifNotExists}; },
            [this](const CreatePhysicalSourceStatement& stmt) -> CoordinatorStatement
            {
                return PlannedCreatePhysicalSource{
                    .logicalSource = stmt.attachedTo,
                    .hostAddr = resolveHost(stmt.host, defaultHost, "SOURCE").getRawValue(),
                    .sourceType = stmt.sourceType,
                    .sourceConfig = CatalogConfig::Options{stmt.sourceConfig},
                    .parserConfig = CatalogConfig::Options{stmt.parserConfig},
                    .ifNotExists = stmt.ifNotExists,
                };
            },
            [this](const CreateSinkStatement& stmt) -> CoordinatorStatement
            {
                return PlannedCreateSink{
                    .name = stmt.name,
                    .hostAddr = resolveHost(stmt.host, defaultHost, "SINK").getRawValue(),
                    .sinkType = stmt.sinkType,
                    .schema = stmt.schema,
                    .config = CatalogConfig::SinkOptions{.sinkConfig = stmt.sinkConfig, .formatConfig = stmt.formatConfig},
                    .ifNotExists = stmt.ifNotExists,
                };
            },
            [](const CreateWorkerStatement& stmt) -> CoordinatorStatement
            {
                if (stmt.maxOperators.has_value() and *stmt.maxOperators > static_cast<size_t>(std::numeric_limits<int32_t>::max()))
                {
                    throw InvalidStatement(
                        "MAX_OPERATORS is {}, but the largest supported value is {}",
                        *stmt.maxOperators,
                        std::numeric_limits<int32_t>::max());
                }
                return PlannedCreateWorker{
                    .hostAddr = stmt.host,
                    .dataAddr = stmt.dataAddress,
                    .maxOperators = stmt.maxOperators.transform([](const size_t value) { return static_cast<int32_t>(value); }),
                    .peers = stmt.downstream,
                    .config = stmt.config,
                    .ifNotExists = stmt.ifNotExists,
                };
            },
            [](const DropLogicalSourceStatement& stmt) -> CoordinatorStatement
            { return PlannedDropLogicalSource{.name = stmt.source.getTarget()}; },
            [](const DropPhysicalSourceStatement& stmt) -> CoordinatorStatement
            { return PlannedDropPhysicalSource{.id = stmt.id.getTarget(), .logicalSource = std::nullopt}; },
            [](const DropSinkStatement& stmt) -> CoordinatorStatement { return PlannedDropSink{.name = stmt.name.getTarget()}; },
            [](const DropQueryStatement& stmt) -> CoordinatorStatement
            { return PlannedDropQuery{.filters = deriveQueryFilters(stmt.queries)}; },
            [](const StopQueryStatement& stmt) -> CoordinatorStatement
            { return PlannedStopQuery{.filters = deriveQueryFilters(stmt.queries)}; },
            [](const DropWorkerStatement& smt) -> CoordinatorStatement { return PlannedDropWorker{.hostAddr = smt.host}; },
            [](const ShowLogicalSourcesStatement& stmt) -> CoordinatorStatement { return PlannedShowLogicalSources{.name = stmt.name}; },
            [](const ShowPhysicalSourcesStatement& stmt) -> CoordinatorStatement
            { return PlannedShowPhysicalSources{.id = stmt.id, .logicalSource = stmt.logicalSource}; },
            [](const ShowSinksStatement& stmt) -> CoordinatorStatement { return PlannedShowSinks{.name = stmt.name}; },
            [](const ShowQueriesStatement& stmt) -> CoordinatorStatement
            { return PlannedShowQueries{.ids = stmt.id.transform([](const QueryId id) { return std::vector{id}; }), .name = stmt.name}; },
            [](const ShowWorkersStatement& stmt) -> CoordinatorStatement { return PlannedShowWorkers{.hostAddr = stmt.host}; },
            [](const CreateModelStatement& stmt) -> CoordinatorStatement
            {
                const auto modelSchema = ModelSchema{.inputs = stmt.inputs, .outputs = stmt.outputs};
                const auto model = RegisteredModel::create(stmt.name, stmt.path, modelSchema);
                return PlannedCreateModel{
                    .name = model.getName(),
                    .path = model.getPath().string(),
                    .imported = model.getImported(),
                    .inputSchema = model.getSchema().inputs,
                    .outputSchema = model.getSchema().outputs,
                    .ifNotExists = stmt.ifNotExists,
                };
            },
            [](const ShowModelsStatement& stmt) -> CoordinatorStatement
            { return PlannedShowModels{.name = stmt.name.transform(&Identifier::asCanonicalString)}; },
            [](const DropModelStatement& stmt) -> CoordinatorStatement { return PlannedDropModel{.name = stmt.name.getTarget()}; },
            [](const ShowVersionStatement& stmt) -> CoordinatorStatement { return PlannedShowVersion{.hostAddr = stmt.host}; },
        },
        *boundStatement);

    return PlanOutput{.statement = std::move(statement), .globalPlan = std::move(globalPlan), .fragments = std::move(fragments)};
}

}
