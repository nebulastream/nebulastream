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
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Util/Overloaded.hpp>
#include <utility>

namespace NES
{

SqlPlanner::SqlPlanner(
    std::shared_ptr<const SourceCatalog> sourceCatalog,
    std::shared_ptr<const SinkCatalog> sinkCatalog,
    std::shared_ptr<const WorkerCatalog> workerCatalog,
    QueryOptimizerConfiguration optimizerConfig)
    : binder([](auto* queryCtx) { return AntlrSQLQueryParser::bindLogicalQueryPlan(queryCtx); })
    , analyzer(sourceCatalog, sinkCatalog)
    , optimizer(optimizerConfig, std::move(sourceCatalog), std::move(sinkCatalog), std::move(workerCatalog))
{
}

std::expected<StatementResult, Exception> SqlPlanner::execute(std::string_view sql) const
{
    auto parsed = binder.parseAndBindSingle(sql);
    if (!parsed)
    {
        return std::unexpected(parsed.error());
    }

    return std::visit(
        Overloaded{
            // --- Queries: run the full pipeline ---
            [this](const QueryStatement& s) -> std::expected<StatementResult, Exception>
            {
                auto analyzed = analyzer.analyse(s.plan);
                auto distributed = optimizer.optimize(analyzed);
                return QueryPlanResult{
                    .name = std::nullopt,
                    .plan = std::move(distributed),
                };
            },
            [this](const ExplainQueryStatement& s) -> std::expected<StatementResult, Exception>
            {
                auto analyzed = analyzer.analyse(s.plan);
                return ExplainQueryResult{.explanation = explain(analyzed, ExplainVerbosity::Debug)};
            },

            // --- DDL: extract parsed parameters ---
            [](const CreateLogicalSourceStatement& s) -> std::expected<StatementResult, Exception>
            {
                return CreateLogicalSourceResult{.name = s.name, .schema = s.schema};
            },
            [](const CreatePhysicalSourceStatement& s) -> std::expected<StatementResult, Exception>
            {
                return CreatePhysicalSourceResult{
                    .logicalSourceName = std::string(s.attachedTo),
                    .sourceType = s.sourceType,
                    .sourceConfig = s.sourceConfig,
                    .parserConfig = s.parserConfig,
                };
            },
            [](const CreateSinkStatement& s) -> std::expected<StatementResult, Exception>
            {
                return CreateSinkResult{
                    .name = s.name,
                    .sinkType = s.sinkType,
                    .schema = s.schema,
                    .sinkConfig = s.sinkConfig,
                    .formatConfig = s.formatConfig,
                };
            },
            [](const CreateWorkerStatement& s) -> std::expected<StatementResult, Exception>
            {
                return CreateWorkerResult{
                    .hostAddr = s.hostAddr,
                    .grpcAddr = s.grpcAddr,
                    .capacity = s.capacity,
                    .peers = s.peers,
                    .config = s.config,
                };
            },

            // --- Drop: extract identifiers ---
            [](const DropLogicalSourceStatement& s) -> std::expected<StatementResult, Exception>
            {
                return DropLogicalSourceResult{.name = std::string(s.source)};
            },
            [](const DropPhysicalSourceStatement& s) -> std::expected<StatementResult, Exception>
            {
                return DropPhysicalSourceResult{.id = s.id};
            },
            [](const DropSinkStatement& s) -> std::expected<StatementResult, Exception>
            {
                return DropSinkResult{.name = s.name};
            },
            [](const DropQueryStatement& s) -> std::expected<StatementResult, Exception>
            {
                return DropQueryResult{.name = s.name, .id = s.id, .stopMode = s.stopMode};
            },
            [](const DropWorkerStatement& s) -> std::expected<StatementResult, Exception>
            {
                return DropWorkerResult{.host = s.host};
            },

            // --- Show: extract filter criteria ---
            [](const ShowLogicalSourcesStatement& s) -> std::expected<StatementResult, Exception>
            {
                return ShowLogicalSourcesResult{.name = s.name};
            },
            [](const ShowPhysicalSourcesStatement& s) -> std::expected<StatementResult, Exception>
            {
                return ShowPhysicalSourcesResult{
                    .logicalSourceName = s.logicalSource ? std::optional(std::string(*s.logicalSource)) : std::nullopt,
                    .id = s.id,
                };
            },
            [](const ShowSinksStatement& s) -> std::expected<StatementResult, Exception>
            {
                return ShowSinksResult{.name = s.name};
            },
            [](const ShowQueriesStatement& s) -> std::expected<StatementResult, Exception>
            {
                return ShowQueriesResult{.name = s.name, .id = s.id};
            },
            [](const ShowWorkersStatement& s) -> std::expected<StatementResult, Exception>
            {
                return ShowWorkersResult{.host = s.host};
            },
            [](const WorkerStatusStatement& s) -> std::expected<StatementResult, Exception>
            {
                return WorkerStatusResult{.host = s.host};
            },
        },
        *parsed);
}

}
