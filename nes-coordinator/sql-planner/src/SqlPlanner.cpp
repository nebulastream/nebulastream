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
    const PlannerContext& ctx,
    QueryOptimizerConfiguration optimizerConfig)
    : binder([](auto* queryCtx) { return AntlrSQLQueryParser::bindLogicalQueryPlan(queryCtx); })
    , analyzer(ctx)
    , optimizer(ctx, WorkerCatalog::getTopology(ctx), std::move(optimizerConfig))
{
}

std::expected<StatementResult, Exception> SqlPlanner::execute(const std::string_view sql) const
{
    auto boundStatement = binder.parseAndBindSingle(sql);
    if (!boundStatement)
    {
        return std::unexpected(boundStatement.error());
    }

    return std::visit(
        Overloaded{
            [this](const QueryStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                auto decomposedPlan = optimizer.optimize(stmt.plan);
                return QueryPlanResult{
                    .name = std::nullopt,
                    .decomposedPlan = std::move(decomposedPlan),
                };
            },
            [this](const ExplainQueryStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ExplainQueryResult{.explanation = explain(analyzer.analyse(stmt.plan), ExplainVerbosity::Debug)};
            },

            [](const CreateLogicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return CreateLogicalSourceResult{.name = stmt.name, .schema = stmt.schema};
            },
            [](const CreatePhysicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return CreatePhysicalSourceResult{
                    .logicalSourceName = stmt.attachedTo.getRawValue(),
                    .sourceType = stmt.sourceType,
                    .sourceConfig = stmt.sourceConfig,
                    .parserConfig = stmt.parserConfig,
                };
            },
            [](const CreateSinkStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return CreateSinkResult{
                    .name = stmt.name,
                    .sinkType = stmt.sinkType,
                    .schema = stmt.schema,
                    .sinkConfig = stmt.sinkConfig,
                    .formatConfig = stmt.formatConfig,
                };
            },
            [](const CreateWorkerStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return CreateWorkerResult{
                    .hostAddr = stmt.hostAddr,
                    .dataAddr = stmt.dataAddr,
                    .capacity = stmt.capacity,
                    .peers = stmt.peers,
                    .config = stmt.config,
                };
            },
            [](const DropLogicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return DropLogicalSourceResult{.name = stmt.source.getRawValue()};
            },
            [](const DropPhysicalSourceStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return DropPhysicalSourceResult{.id = stmt.id};
            },
            [](const DropSinkStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return DropSinkResult{.name = stmt.name};
            },
            [](const DropQueryStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return DropQueryResult{.name = stmt.name, .id = stmt.id, .stopMode = stmt.stopMode};
            },
            [](const DropWorkerStatement& smt) -> std::expected<StatementResult, Exception>
            {
                return DropWorkerResult{.host = smt.host};
            },
            [](const ShowLogicalSourcesStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowLogicalSourcesResult{.name = stmt.name};
            },
            [](const ShowPhysicalSourcesStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowPhysicalSourcesResult{
                    .logicalSourceName = stmt.logicalSource ? std::optional(stmt.logicalSource->getRawValue()) : std::nullopt,
                    .id = stmt.id,
                };
            },
            [](const ShowSinksStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowSinksResult{.name = stmt.name};
            },
            [](const ShowQueriesStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowQueriesResult{.name = stmt.name, .id = stmt.id};
            },
            [](const ShowWorkersStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return ShowWorkersResult{.host = stmt.host};
            },
            [](const WorkerStatusStatement& stmt) -> std::expected<StatementResult, Exception>
            {
                return WorkerStatusResult{.host = stmt.host};
            },
        },
        *boundStatement);
}

}
