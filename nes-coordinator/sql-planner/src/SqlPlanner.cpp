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

#include <expected>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

#include <Model.hpp>

namespace NES
{
namespace {
    /// The `*Result` structs are serialized to JSON for the Rust coordinator, where these strings become catalog keys.
    /// They must therefore carry the canonical (SQL case-folded) spelling, never `getOriginalString()`.
    std::optional<std::string> canonical(const std::optional<Identifier> &identifier) {
        return identifier.transform([](const Identifier &value) { return value.asCanonicalString(); });
    }

    std::unordered_map<std::string, std::string> canonicalKeys(
        const std::unordered_map<Identifier, std::string> &config) {
        return config | std::views::transform([](const auto &entry) {
                   return std::pair{entry.first.asCanonicalString(), entry.second};
               })
               | std::ranges::to<std::unordered_map<std::string, std::string> >();
    }

    rfl::Generic toGeneric(const ConfigObject &config) {
        rfl::Generic::Object object;
        for (const auto &[key, value]: config) {
            object.insert(key, rfl::Generic(value));
        }
        return rfl::Generic(object);
    }

    /// SQL nests the output-formatter options inside the sink's config options, and the catalog stores that same shape.
    rfl::Generic mergeFormatConfig(const ConfigObject &sinkConfig, const ConfigObject &formatConfig) {
        rfl::Generic::Object object;
        for (const auto &[key, value]: sinkConfig) {
            object.insert(key, rfl::Generic(value));
        }
        if (!formatConfig.empty()) {
            object.insert(std::string{"OUTPUT_FORMATTER"}, toGeneric(formatConfig));
        }
        return rfl::Generic(object);
    }

    std::expected<std::string, Exception>
    resolveHost(const std::optional<Host> &host, const HostPolicy &hostPolicy, const std::string_view subject) {
        if (host.has_value()) {
            return host->getRawValue();
        }
        return std::visit(
            Overloaded{
                [](const DefaultHost &defaultHost) -> std::expected<std::string, Exception> {
                    return defaultHost.hostName;
                },
                [subject](const RequireHostConfig &) -> std::expected<std::string, Exception> {
                    return std::unexpected{InvalidStatement("`{}`.`HOST` was not set", subject)};
                }
            },
            hostPolicy);
    }
}

SqlPlanner::SqlPlanner(const PlannerContext &ctx, QueryOptimizerConfiguration optimizerConfig, HostPolicy hostPolicy)
    : binder{[](auto *queryCtx) { return AntlrSQLQueryParser::bindLogicalQueryPlan(queryCtx); }}
      , analyzer(ctx)
      , optimizer(std::move(optimizerConfig), ctx)
      , hostPolicy(std::move(hostPolicy)) {
}

std::expected<PlanOutput, Exception> SqlPlanner::plan(const std::string_view sql) const {
    auto boundStatement = binder.parseAndBindSingle(sql);
    if (!boundStatement) {
        return std::unexpected(boundStatement.error());
    }

    /// Only a query produces fragments; every other statement is a pure catalog request.
    std::unordered_map<Host, std::vector<LogicalPlan> > fragments;

    auto statement = std::visit(
        Overloaded{
            [this, sql, &fragments](const QueryStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                fragments = optimizer.optimize(stmt.plan);
                return CreateQueryResult{.name = canonical(stmt.name), .sql = std::string{sql}};
            },
            [this](const ExplainQueryStatement& stmt) -> std::expected<StatementResult, Exception> {
                return ExplainQueryResult{.explanation = explain(analyzer.analyse(stmt.plan), ExplainVerbosity::Debug)};
            },

            [](const CreateLogicalSourceStatement &stmt) -> std::expected<StatementResult, Exception> {
                return CreateLogicalSourceResult{.name = stmt.name.asCanonicalString(), .schema = reflect(stmt.schema)};
            },
            [this](const CreatePhysicalSourceStatement &stmt) -> std::expected<StatementResult, Exception> {
                auto host = resolveHost(stmt.host, hostPolicy, "SOURCE");
                if (!host) {
                    return std::unexpected(host.error());
                }
                return CreatePhysicalSourceResult{
                    .logicalSourceName = stmt.attachedTo.asCanonicalString(),
                    .host = std::move(*host),
                    .sourceType = stmt.sourceType.asCanonicalString(),
                    .sourceConfig = canonicalKeys(stmt.sourceConfig),
                    .parserConfig = canonicalKeys(stmt.parserConfig),
                };
            },
            [this](const CreateSinkStatement &stmt) -> std::expected<StatementResult, Exception> {
                auto host = resolveHost(stmt.host, hostPolicy, "SINK");
                if (!host) {
                    return std::unexpected(host.error());
                }
                return CreateSinkResult{
                    .name = stmt.name.asCanonicalString(),
                    .host = std::move(*host),
                    .sinkType = stmt.sinkType.asCanonicalString(),
                    .schema = reflect(stmt.schema),
                    .config = mergeFormatConfig(canonicalKeys(stmt.sinkConfig), canonicalKeys(stmt.formatConfig)),
                };
            },
            [](const CreateWorkerStatement &stmt) -> std::expected<StatementResult, Exception> {
                return CreateWorkerResult{
                    .hostAddr = stmt.hostAddr,
                    .dataAddr = stmt.dataAddr,
                    .maxOperators = stmt.maxOperators.transform([](const size_t value) { return static_cast<int32_t>(value); }),
                    .peers = stmt.downstream,
                    .config = stmt.config,
                };
            },
            [](const DropLogicalSourceStatement& stmt) -> std::expected<StatementResult, Exception> {
                return DropLogicalSourceResult{.name = stmt.source.asCanonicalString()}; },
            [](const DropPhysicalSourceStatement& stmt) -> std::expected<StatementResult, Exception> {
                return DropPhysicalSourceResult{.id = stmt.id, .logicalSourceName = std::nullopt}; },
            [](const DropSinkStatement& stmt) -> std::expected<StatementResult, Exception> {
                return DropSinkResult{.name = stmt.name.asCanonicalString()};
            },
            [](const DropQueryStatement &stmt) -> std::expected<StatementResult, Exception> {
                return DropQueryResult{
                    .filters = QueryFilters{
                        .ids = stmt.id.transform([](const uint64_t id) { return std::vector<uint64_t>{id}; }),
                        .name = canonical(stmt.name)}};
            },
            [](const DropWorkerStatement &smt) -> std::expected<StatementResult, Exception> { return DropWorkerResult{.host = smt.host}; },
            [](const ShowLogicalSourcesStatement& stmt) -> std::expected<StatementResult, Exception> {
                return ShowLogicalSourcesResult{.name = canonical(stmt.name)};
            },
            [](const ShowPhysicalSourcesStatement &stmt) -> std::expected<StatementResult, Exception> {
                return ShowPhysicalSourcesResult{
                    .id = stmt.id.transform([](const uint32_t id) { return static_cast<uint64_t>(id); }),
                    .logicalSourceName = canonical(stmt.logicalSource),
                };
            },
            [](const ShowSinksStatement &stmt) -> std::expected<StatementResult, Exception> {
                return ShowSinksResult{.name = canonical(stmt.name)};
            },
            [](const ShowQueriesStatement &stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowQueriesResult{
                    .ids = stmt.id.transform([](const uint64_t id) { return std::vector<uint64_t>{id}; }),
                    .name = canonical(stmt.name)
                };
            },
            [](const ShowWorkersStatement &stmt) -> std::expected<StatementResult, Exception> {
                return ShowWorkersResult{.host = stmt.host};
            },
            /// Rust's `GetWorkerStatus` takes a single mandatory address and cannot express a host list, so this degrades to
            /// listing every worker. TODO: give worker status its own Rust variant.
            [](const WorkerStatusStatement &) -> std::expected<StatementResult, Exception> {
                return ShowWorkersResult{.host = std::nullopt};
            },
            [](const CreateModelStatement &stmt) -> std::expected<StatementResult, Exception> {
                const auto modelSchema = ModelSchema{.inputs = stmt.inputs, .outputs = stmt.outputs};
                const auto model = RegisteredModel::create(stmt.name, stmt.path, modelSchema);
                return CreateModelResult{
                    .name = model.getName(),
                    .path = model.getPath().string(),
                    .imported = rfl::Generic(reflect(model.getImported())),
                    .inputs = rfl::Generic(reflect(model.getSchema().inputs)),
                    .outputs = rfl::Generic(reflect(model.getSchema().outputs)),
                };
            },
            [](const ShowModelsStatement &stmt) -> std::expected<StatementResult, Exception> {
                return ShowModelsResult{.name = canonical(stmt.name)};
            },
            [](const DropModelStatement &stmt) -> std::expected<StatementResult, Exception> { return DropModelResult{.name = stmt.name}; },
        },
        *boundStatement);

    if (!statement)
    {
        return std::unexpected(statement.error());
    }
    return PlanOutput{.statement = std::move(*statement), .fragments = std::move(fragments)};
}

}
