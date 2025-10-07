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

#include <Statements/StatementHandler.hpp>

#include <algorithm>
#include <expected>
#include <memory>
#include <mutex>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Util/Common.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

SourceStatementHandler::SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog) : sourceCatalog(sourceCatalog)
{
}

std::expected<CreateLogicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreateLogicalSourceStatement& statement)
{
    if (const auto created = sourceCatalog->addLogicalSource(statement.name, statement.schema))
    {
        return CreateLogicalSourceStatementResult{created.value()};
    }
    return std::unexpected{SourceAlreadyExists(statement.name)};
}

std::expected<CreatePhysicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreatePhysicalSourceStatement& statement)
{
    auto logicalSource = sourceCatalog->getLogicalSource(statement.attachedTo.getRawValue());
    if (!logicalSource)
    {
        return std::unexpected{UnknownSourceName(statement.attachedTo.getRawValue())};
    }

    if (const auto created = sourceCatalog->addPhysicalSource(
            *logicalSource, statement.sourceType, statement.workerId, statement.sourceConfig, statement.parserConfig))
    {
        return CreatePhysicalSourceStatementResult{created.value()};
    }
    return std::unexpected{InvalidConfigParameter("Invalid configuration: {}", statement)};
}

std::expected<ShowLogicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowLogicalSourcesStatement& statement) const
{
    if (statement.name)
    {
        if (const auto foundSource = sourceCatalog->getLogicalSource(*statement.name))
        {
            return ShowLogicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowLogicalSourcesStatementResult{{}};
    }
    return ShowLogicalSourcesStatementResult{sourceCatalog->getAllLogicalSources() | std::ranges::to<std::vector>()};
}

std::expected<ShowPhysicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowPhysicalSourcesStatement& statement) const
{
    if (statement.id and not statement.logicalSource)
    {
        if (const auto foundSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{statement.id.value()}))
        {
            return ShowPhysicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (not statement.id and statement.logicalSource)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(statement.logicalSource->getRawValue()))
        {
            if (const auto foundSources = sourceCatalog->getPhysicalSources(*logicalSource))
            {
                return ShowPhysicalSourcesStatementResult{*foundSources | std::ranges::to<std::vector>()};
            }
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (statement.logicalSource and statement.id)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(statement.logicalSource->getRawValue()))
        {
            if (const auto foundSources = sourceCatalog->getPhysicalSources(*logicalSource))
            {
                return ShowPhysicalSourcesStatementResult{
                    foundSources.value()
                    | std::views::filter([statement](const auto& source)
                                         { return source.getPhysicalSourceId() == PhysicalSourceId{statement.id.value()}; })
                    | std::ranges::to<std::vector>()};
            }
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    return ShowPhysicalSourcesStatementResult{
        sourceCatalog->getLogicalToPhysicalSourceMapping() | std::views::transform([](auto& pair) { return pair.second; })
        | std::views::join | std::ranges::to<std::vector>()};
}

std::expected<DropLogicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropLogicalSourceStatement& statement)
{
    if (auto logical = sourceCatalog->getLogicalSource(statement.source.getRawValue()))
    {
        if (sourceCatalog->removeLogicalSource(*logical))
        {
            return DropLogicalSourceStatementResult{statement.source, *logical->getSchema()};
        }
    }
    return std::unexpected{UnknownSourceName(statement.source.getRawValue())};
}

std::expected<DropPhysicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropPhysicalSourceStatement& statement)
{
    if (sourceCatalog->removePhysicalSource(statement.descriptor))
    {
        return DropPhysicalSourceStatementResult{statement.descriptor};
    }
    return std::unexpected{UnknownSourceName("Unknown physical source: {}", statement.descriptor)};
}

SinkStatementHandler::SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog) : sinkCatalog(sinkCatalog)
{
}

std::expected<CreateSinkStatementResult, Exception> SinkStatementHandler::operator()(const CreateSinkStatement& statement)
{
    if (const auto created
        = sinkCatalog->addSinkDescriptor(statement.name, statement.schema, statement.sinkType, statement.workerId, statement.sinkConfig))
    {
        return CreateSinkStatementResult{created.value()};
    }
    return std::unexpected{SinkAlreadyExists(statement.name)};
}

std::expected<ShowSinksStatementResult, Exception> SinkStatementHandler::operator()(const ShowSinksStatement& statement) const
{
    if (statement.name)
    {
        if (const auto foundSink = sinkCatalog->getSinkDescriptor(*statement.name))
        {
            return ShowSinksStatementResult{std::vector{*foundSink}};
        }
        return ShowSinksStatementResult{{}};
    }
    return ShowSinksStatementResult{sinkCatalog->getAllSinkDescriptors()};
}

std::expected<DropSinkStatementResult, Exception> SinkStatementHandler::operator()(const DropSinkStatement& statement)
{
    if (sinkCatalog->removeSinkDescriptor(statement.descriptor))
    {
        return DropSinkStatementResult{statement.descriptor};
    }
    return std::unexpected{UnknownSinkName(statement.descriptor.getSinkName())};
}

std::expected<DropQueryStatementResult, Exception> QueryStatementHandler::operator()(const DropQueryStatement& statement)
{
    auto stopResult = queryManager->stop(statement.id)
                          .and_then([&statement, this] { return queryManager->unregister(statement.id); })
                          .transform_error(
                              [](auto vecOfErrors)
                              {
                                  return QueryStopFailed(
                                      "Could not stop query: {}",
                                      fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
                              })
                          .transform([&statement] { return DropQueryStatementResult{statement.id}; });

    if (statement.blocking)
    {
        while (true)
        {
            auto statusResult = queryManager->status(statement.id)
                                    .transform(
                                        [](const DistributedQueryStatus& status)
                                        {
                                            return status.getGlobalQueryState() == QueryState::Failed
                                                || status.getGlobalQueryState() == QueryState::Stopped;
                                        });
            if (!statusResult)
            {
                return std::unexpected(QueryStopFailed(
                    "while waiting for query status to change to stopped or failed: {}",
                    fmt::join(
                        std::views::transform(statusResult.error(), [](const Exception& exception) { return exception.what(); }), "; ")));
            }
            if (*statusResult)
            {
                return stopResult;
            }
            NES_DEBUG("Query has not been stopped yet. Waiting...");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    return stopResult;
}

QueryStatementHandler::QueryStatementHandler(
    SharedPtr<QueryManager> queryManager,
    SharedPtr<SourceCatalog> sourceCatalog,
    SharedPtr<SinkCatalog> sinkCatalog,
    SharedPtr<WorkerCatalog> workerCatalog)
    : queryManager(std::move(queryManager))
    , sourceCatalog(std::move(sourceCatalog))
    , sinkCatalog(std::move(sinkCatalog))
    , workerCatalog(std::move(workerCatalog))
{
}

std::expected<ExplainQueryStatementResult, Exception> QueryStatementHandler::operator()(const ExplainQueryStatement& statement)
{
    CPPTRACE_TRY
    {
        auto boundPlan = PlanStage::BoundLogicalPlan{statement.plan};
        QueryPlanningContext context{
            .id = INVALID<LocalQueryId>,
            .sqlString = statement.plan.getOriginalSql(),
            .sourceCatalog = Util::copyPtr(sourceCatalog),
            .sinkCatalog = Util::copyPtr(sinkCatalog),
            .workerCatalog = Util::copyPtr(workerCatalog)};

        std::stringstream explainMessage;
        fmt::println(explainMessage, "Query:\n{}", statement.plan.getOriginalSql());
        fmt::println(explainMessage, "Initial Logical Plan:\n{}", boundPlan.plan);
        const auto distributedPlan = QueryPlanner::with(context).plan(std::move(boundPlan));

        fmt::println(explainMessage, "Optimized Global Plan:\n{}", distributedPlan.getGlobalPlan());

        fmt::println(explainMessage, "Decomposed Plans:");
        for (const auto& [worker, plans] : distributedPlan)
        {
            fmt::println(explainMessage, "{} plans on {}:", plans.size(), worker);
            for (const auto& [index, plan] : plans | views::enumerate)
            {
                fmt::println(explainMessage, "{}:\n{}\n", index, plan);
            }
        }
        return ExplainQueryStatementResult{explainMessage.str()};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<QueryStatementResult, Exception> QueryStatementHandler::operator()(const QueryStatement& statement)
{
    CPPTRACE_TRY
    {
        auto boundPlan = PlanStage::BoundLogicalPlan{statement.plan};
        QueryPlanningContext context{
            .id = INVALID<LocalQueryId>,
            .sqlString = statement.plan.getOriginalSql(),
            .sourceCatalog = Util::copyPtr(sourceCatalog),
            .sinkCatalog = Util::copyPtr(sinkCatalog),
            .workerCatalog = Util::copyPtr(workerCatalog)};

        const auto distributedPlan = QueryPlanner::with(context).plan(std::move(boundPlan));
        const auto queryResult = queryManager->registerQuery(distributedPlan);
        return queryResult
            .and_then(
                [this](const auto& query)
                {
                    return queryManager->start(query)
                        .transform([&query] { return query; })
                        .transform_error(
                            [](auto vecOfErrors)
                            {
                                return QueryStartFailed(
                                    "Could not start query: {}",
                                    fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
                            });
                })
            .transform([](auto query) { return QueryStatementResult{query}; });
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

TopologyStatementHandler::TopologyStatementHandler(SharedPtr<QueryManager> queryManager, SharedPtr<WorkerCatalog> workerCatalog)
    : queryManager(std::move(queryManager)), workerCatalog(std::move(workerCatalog))
{
}

std::expected<WorkerStatusStatementResult, Exception> TopologyStatementHandler::operator()(const WorkerStatusStatement& statement)
{
    if (statement.host.empty())
    {
        /// Fetch all
        return std::unexpected(UnknownException("WorkerStatusStatement not fully implemented"));
    }
    return std::unexpected(UnknownException("WorkerStatusStatement not fully implemented"));
}

std::expected<CreateWorkerStatementResult, Exception> TopologyStatementHandler::operator()(const CreateWorkerStatement& statement)
{
    workerCatalog->addWorker(
        HostAddr(statement.host),
        GrpcAddr(statement.grpc),
        statement.capacity,
        statement.downstream | std::views::transform([](auto connection) { return HostAddr(std::move(connection)); })
            | std::ranges::to<std::vector>());
    return CreateWorkerStatementResult{WorkerId(statement.host)};
}

std::expected<DropWorkerStatementResult, Exception> TopologyStatementHandler::operator()(const DropWorkerStatement& statement)
{
    const auto workerConfigOpt = workerCatalog->removeWorker(HostAddr(statement.host));
    if (workerConfigOpt)
    {
        return DropWorkerStatementResult{WorkerId(workerConfigOpt->host.getRawValue())};
    }
    return std::unexpected(UnknownWorker(": '{}'", statement.host));
}

std::expected<ShowQueriesStatementResult, Exception> QueryStatementHandler::operator()(const ShowQueriesStatement& statement)
{
    if (not statement.id.has_value())
    {
        auto statusResults
            = queryManager->queries()
            | std::views::transform(
                  [&](const auto& queryId) -> std::pair<DistributedQueryId, std::expected<DistributedQueryStatus, Exception>>
                  {
                      auto statusResult = queryManager->status(queryId).transform_error(
                          [](auto vecOfErrors)
                          {
                              return QueryStopFailed(
                                  "Could not stop query: {}",
                                  fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
                          });
                      return {queryId, statusResult};
                  })
            | std::ranges::to<std::vector>();

        auto failedStatusResults = statusResults
            | std::views::filter([](const auto& idAndStatusResult) { return !idAndStatusResult.second.has_value(); })
            | std::views::transform([](const auto& idAndStatusResult) -> std::pair<DistributedQueryId, Exception>
                                    { return {idAndStatusResult.first, idAndStatusResult.second.error()}; });

        auto goodQueryStatusResults = statusResults
            | std::views::filter([](const auto& idAndStatusResult) { return idAndStatusResult.second.has_value(); })
            | std::views::transform([](const auto& idAndStatusResult) -> std::pair<DistributedQueryId, DistributedQueryStatus>
                                    { return {idAndStatusResult.first, idAndStatusResult.second.value()}; });
        if (!failedStatusResults.empty())
        {
            return std::unexpected(
                QueryStatusFailed("Could not retrieve query status for some queries: ", fmt::join(failedStatusResults, "\n")));
        }

        return ShowQueriesStatementResult{
            goodQueryStatusResults | std::ranges::to<std::unordered_map<DistributedQueryId, DistributedQueryStatus>>()};
    }

    if (const auto statusOpt = queryManager->status(statement.id.value()); statusOpt.has_value())
    {
        return ShowQueriesStatementResult{
            std::unordered_map<DistributedQueryId, DistributedQueryStatus>{{statement.id.value(), statusOpt.value()}}};
    }
    return ShowQueriesStatementResult{.queries = {}};
}

}
