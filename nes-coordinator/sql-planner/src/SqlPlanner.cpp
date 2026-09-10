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
#include <expected>
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
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Ranges.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>

#include <CatalogConfig.hpp>
#include <Model.hpp>

namespace NES
{
namespace
{
/// The `*Result` structs are serialized to JSON for the Rust coordinator, where these strings become catalog keys.
/// They must therefore carry the canonical (SQL case-folded) spelling, never `getOriginalString()`.
std::optional<std::string> canonical(const std::optional<Identifier>& identifier)
{
    return identifier.transform([](const Identifier& value) { return value.asCanonicalString(); });
}

rfl::Generic toGeneric(const ConfigObject& config)
{
    rfl::Generic::Object object;
    for (const auto& [key, value] : config)
    {
        object.insert(key, rfl::Generic(value));
    }
    return rfl::Generic(object);
}

/// SQL nests the output-formatter options inside the sink's config options, and the catalog stores that same shape.
rfl::Generic mergeFormatConfig(const ConfigObject& sinkConfig, const ConfigObject& formatConfig)
{
    rfl::Generic::Object object;
    for (const auto& [key, value] : sinkConfig)
    {
        object.insert(key, rfl::Generic(value));
    }
    if (!formatConfig.empty())
    {
        object.insert(std::string{CatalogConfig::OUTPUT_FORMATTER_KEY}, toGeneric(formatConfig));
    }
    return rfl::Generic(object);
}

/// Section order is fixed here rather than taken from the order the statement listed the stages in.
/// The rewrite and the split are computed once so that operator ids match across sections.
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
    const auto globalPlan = optimizer.optimizeGlobalPlan(statement.plan);
    const auto decomposedPlans = optimizer.place(globalPlan);

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
        /// The placement result is stored in an unordered map. Sort by host for deterministic output.
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

SqlPlanner::SqlPlanner(const std::shared_ptr<Catalog>& catalog, QueryOptimizerConfiguration optimizerConfig, HostPolicy hostPolicy)
    : binder{[](auto* queryCtx) { return AntlrSQLQueryParser::bindLogicalQueryPlan(queryCtx); }}
    , optimizer{std::move(optimizerConfig), catalog}
    , hostPolicy{std::move(hostPolicy)}
{
}

std::expected<PlanOutput, Exception> SqlPlanner::plan(const std::string_view sql) const
{
    auto boundStatement = binder.parseAndBindSingle(sql);
    if (!boundStatement)
    {
        return std::unexpected(boundStatement.error());
    }

    /// Only a query produces fragments; every other statement is a pure catalog request.
    std::unordered_map<Host, std::vector<LogicalPlan>> fragments;

    auto statement = std::visit(
        Overloaded{
            [this, sql, &fragments](const QueryStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                fragments = optimizer.optimize(stmt.plan);
                return CreateQueryResult{.name = canonical(stmt.name), .sql = std::string{sql}};
            },
            [this](const ExplainQueryStatement& stmt) -> std::expected<StatementResult, Exception>
            { return ExplainQueryResult{.explanation = computeExplainOutput(stmt, optimizer)}; },

            [](const CreateLogicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return CreateLogicalSourceResult{.name = stmt.name.asCanonicalString(), .schema = ReflectionContext{}.reflect(stmt.schema)};
            },
            [this](const CreatePhysicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                auto host = resolveHost(stmt.host, hostPolicy, "SOURCE");
                if (!host)
                {
                    return std::unexpected(host.error());
                }
                return CreatePhysicalSourceResult{
                    .logicalSourceName = stmt.attachedTo.asCanonicalString(),
                    .host = std::move(*host),
                    .sourceType = stmt.sourceType.asCanonicalString(),
                    .sourceConfig = CatalogConfig::toStringKeys(stmt.sourceConfig),
                    .parserConfig = CatalogConfig::toStringKeys(stmt.parserConfig),
                };
            },
            [this](const CreateSinkStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                auto host = resolveHost(stmt.host, hostPolicy, "SINK");
                if (!host)
                {
                    return std::unexpected(host.error());
                }
                return CreateSinkResult{
                    .name = stmt.name.asCanonicalString(),
                    .host = std::move(*host),
                    .sinkType = stmt.sinkType.asCanonicalString(),
                    .schema = ReflectionContext{}.reflect(stmt.schema),
                    .config
                    = mergeFormatConfig(CatalogConfig::toStringKeys(stmt.sinkConfig), CatalogConfig::toStringKeys(stmt.formatConfig)),
                };
            },
            [](const CreateWorkerStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return CreateWorkerResult{
                    .hostAddr = stmt.host,
                    .dataAddr = stmt.dataAddress,
                    .maxOperators = stmt.maxOperators.transform([](const size_t value) { return static_cast<int32_t>(value); }),
                    .peers = stmt.downstream,
                    .config = stmt.config,
                };
            },
            [](const DropLogicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            { return DropLogicalSourceResult{.name = canonical(stmt.source)}; },
            [](const DropPhysicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            { return DropPhysicalSourceResult{.id = stmt.id, .logicalSourceName = std::nullopt}; },
            [](const DropSinkStatement& stmt) -> std::expected<StatementResult, Exception>
            { return DropSinkResult{.name = canonical(stmt.name)}; },
            [](const DropQueryStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return DropQueryResult{
                    .filters = QueryFilters{
                        .ids = stmt.id.transform([](const uint64_t id) { return std::vector<uint64_t>{id}; }),
                        .name = canonical(stmt.name)}};
            },
            [](const DropWorkerStatement& smt) -> std::expected<StatementResult, Exception> { return DropWorkerResult{.host = smt.host}; },
            [](const ShowLogicalSourcesStatement& stmt) -> std::expected<StatementResult, Exception>
            { return ShowLogicalSourcesResult{.name = canonical(stmt.name)}; },
            [](const ShowPhysicalSourcesStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowPhysicalSourcesResult{
                    .id = stmt.id.transform([](const uint32_t id) { return static_cast<uint64_t>(id); }),
                    .logicalSourceName = canonical(stmt.logicalSource),
                };
            },
            [](const ShowSinksStatement& stmt) -> std::expected<StatementResult, Exception>
            { return ShowSinksResult{.name = canonical(stmt.name)}; },
            [](const ShowQueriesStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowQueriesResult{
                    .ids = stmt.id.transform([](const uint64_t id) { return std::vector<uint64_t>{id}; }), .name = canonical(stmt.name)};
            },
            [](const ShowWorkersStatement& stmt) -> std::expected<StatementResult, Exception>
            { return ShowWorkersResult{.host = stmt.host}; },
            /// Rust's `GetWorkerStatus` takes a single mandatory address and cannot express a host list, so this degrades to
            /// listing every worker. Worker status could get its own Rust variant.
            [](const WorkerStatusStatement&) -> std::expected<StatementResult, Exception>
            { return ShowWorkersResult{.host = std::nullopt}; },
            [](const CreateModelStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                const auto modelSchema = ModelSchema{.inputs = stmt.inputs, .outputs = stmt.outputs};
                const auto model = RegisteredModel::create(stmt.name, stmt.path, modelSchema);
                return CreateModelResult{
                    .name = model.getName(),
                    .path = model.getPath().string(),
                    .imported = rfl::Generic(ReflectionContext{}.reflect(model.getImported())),
                    .inputs = rfl::Generic(ReflectionContext{}.reflect(model.getSchema().inputs)),
                    .outputs = rfl::Generic(ReflectionContext{}.reflect(model.getSchema().outputs)),
                };
            },
            [](const ShowModelsStatement& stmt) -> std::expected<StatementResult, Exception>
            { return ShowModelsResult{.name = canonical(stmt.name)}; },
            [](const DropModelStatement& stmt) -> std::expected<StatementResult, Exception> { return DropModelResult{.name = stmt.name}; },
            /// Names no worker, so the coordinator asks every one it has.
            [](const ShowVersionStatement&) -> std::expected<StatementResult, Exception>
            { return ShowVersionResult{.host = std::nullopt}; },
        },
        *boundStatement);

    if (!statement)
    {
        return std::unexpected(statement.error());
    }
    return PlanOutput{.statement = std::move(*statement), .fragments = std::move(fragments)};
}

}
