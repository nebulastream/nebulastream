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
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
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
/// The planned statements are serialized to JSON for the Rust coordinator, where these strings become catalog keys.
/// They must therefore use the canonical (SQL case-folded) spelling, never the user's spelling.
std::optional<std::string> canonical(const std::optional<Identifier>& identifier)
{
    return identifier.transform([](const Identifier& value) { return value.asCanonicalString(); });
}

/// Collects what the coordinator records about a placed query: one entry per fragment,
/// and the ids of every source and sink that the query reads and writes.
void recordPlacement(
    const std::unordered_map<Host, std::vector<LogicalPlan>>& placed,
    std::vector<PlannedFragment>& fragments,
    std::vector<PhysicalSourceId>& sourceIds,
    std::vector<SinkId>& sinkIds)
{
    for (const auto& [host, plans] : placed)
    {
        for (const auto& plan : plans)
        {
            const auto sinks = getOperatorByType<SinkLogicalOperator>(plan);
            const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(plan);
            for (const auto& source : sources)
            {
                sourceIds.push_back(source->getSourceDescriptor().getPhysicalSourceId());
            }
            for (const auto& sink : sinks)
            {
                if (const auto& descriptor = sink->getSinkDescriptor(); descriptor.has_value())
                {
                    sinkIds.push_back(descriptor->getSinkId());
                }
            }
            fragments.push_back(PlannedFragment{
                .host = host,
                .plan = plan,
                .numOperators = static_cast<int32_t>(flatten(plan).size() - sinks.size() - sources.size()),
                .hasSource = !sources.empty(),
            });
        }
    }
}

/// Section order is fixed here rather than taken from the statement's stage order.
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

    /// Only a query is placed; every other statement is a pure catalog request.
    std::vector<PlannedFragment> fragments;
    std::vector<PhysicalSourceId> sourceIds;
    std::vector<SinkId> sinkIds;

    auto statement = std::visit(
        Overloaded{
            [&](const QueryStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                recordPlacement(optimizer.optimize(stmt.plan), fragments, sourceIds, sinkIds);
                return PlannedCreateQuery{.name = canonical(stmt.name), .sql = std::string{sql}};
            },
            [this](const ExplainQueryStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedExplainQuery{.explanation = computeExplainOutput(stmt, optimizer)}; },

            [](const CreateLogicalSourceStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                return PlannedCreateLogicalSource{
                    .name = stmt.name.asCanonicalString(), .schema = ReflectionContext{}.reflect(stmt.schema)};
            },
            [this](const CreatePhysicalSourceStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                auto host = resolveHost(stmt.host, hostPolicy, "SOURCE");
                if (!host)
                {
                    return std::unexpected(host.error());
                }
                return PlannedCreatePhysicalSource{
                    .logicalSourceName = stmt.attachedTo.asCanonicalString(),
                    .host = std::move(*host),
                    .sourceType = stmt.sourceType.asCanonicalString(),
                    .sourceConfig = CatalogConfig::toStringKeys(stmt.sourceConfig),
                    .parserConfig = CatalogConfig::toStringKeys(stmt.parserConfig),
                };
            },
            [this](const CreateSinkStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                auto host = resolveHost(stmt.host, hostPolicy, "SINK");
                if (!host)
                {
                    return std::unexpected(host.error());
                }
                return PlannedCreateSink{
                    .name = stmt.name.asCanonicalString(),
                    .host = std::move(*host),
                    .sinkType = stmt.sinkType.asCanonicalString(),
                    .schema = ReflectionContext{}.reflect(stmt.schema),
                    .config = CatalogConfig::mergeFormatConfig(
                        CatalogConfig::toStringKeys(stmt.sinkConfig), CatalogConfig::toStringKeys(stmt.formatConfig)),
                };
            },
            [](const CreateWorkerStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                return PlannedCreateWorker{
                    .hostAddr = stmt.host,
                    .dataAddr = stmt.dataAddress,
                    .maxOperators = stmt.maxOperators.transform([](const size_t value) { return static_cast<int32_t>(value); }),
                    .peers = stmt.downstream,
                    .config = stmt.config,
                };
            },
            [](const DropLogicalSourceStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedDropLogicalSource{.name = canonical(stmt.source)}; },
            [](const DropPhysicalSourceStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedDropPhysicalSource{.id = stmt.id, .logicalSourceName = std::nullopt}; },
            [](const DropSinkStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedDropSink{.name = canonical(stmt.name)}; },
            [](const DropQueryStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                return PlannedDropQuery{
                    .filters = QueryFilters{
                        .ids = stmt.id.transform([](const uint64_t id) { return std::vector<uint64_t>{id}; }),
                        .name = canonical(stmt.name)}};
            },
            [](const DropWorkerStatement& smt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedDropWorker{.host = smt.host}; },
            [](const ShowLogicalSourcesStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedShowLogicalSources{.name = canonical(stmt.name)}; },
            [](const ShowPhysicalSourcesStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                return PlannedShowPhysicalSources{
                    .id = stmt.id.transform([](const uint32_t id) { return static_cast<uint64_t>(id); }),
                    .logicalSourceName = canonical(stmt.logicalSource),
                };
            },
            [](const ShowSinksStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedShowSinks{.name = canonical(stmt.name)}; },
            [](const ShowQueriesStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                return PlannedShowQueries{
                    .ids = stmt.id.transform([](const uint64_t id) { return std::vector<uint64_t>{id}; }), .name = canonical(stmt.name)};
            },
            [](const ShowWorkersStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedShowWorkers{.host = stmt.host}; },
            /// The coordinator's worker-status request cannot express a host list, so this lists every worker instead.
            [](const WorkerStatusStatement&) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedShowWorkers{.host = std::nullopt}; },
            [](const CreateModelStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            {
                const auto modelSchema = ModelSchema{.inputs = stmt.inputs, .outputs = stmt.outputs};
                const auto model = RegisteredModel::create(stmt.name, stmt.path, modelSchema);
                return PlannedCreateModel{
                    .name = model.getName(),
                    .path = model.getPath().string(),
                    .imported = rfl::Generic(ReflectionContext{}.reflect(model.getImported())),
                    .inputs = rfl::Generic(ReflectionContext{}.reflect(model.getSchema().inputs)),
                    .outputs = rfl::Generic(ReflectionContext{}.reflect(model.getSchema().outputs)),
                };
            },
            [](const ShowModelsStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedShowModels{.name = canonical(stmt.name)}; },
            [](const DropModelStatement& stmt) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedDropModel{.name = stmt.name}; },
            [](const ShowVersionStatement&) -> std::expected<CoordinatorStatement, Exception>
            { return PlannedShowVersion{.host = std::nullopt}; },
        },
        *boundStatement);

    if (!statement)
    {
        return std::unexpected(statement.error());
    }
    return PlanOutput{
        .statement = std::move(*statement),
        .fragments = std::move(fragments),
        .sourceIds = std::move(sourceIds),
        .sinkIds = std::move(sinkIds)};
}

}
