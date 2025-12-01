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
#include <ranges>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Pointers.hpp>
#include <Util/Ranges.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

SourceStatementHandler::SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog, HostPolicy hostPolicy)
    : sourceCatalog(sourceCatalog), hostPolicy(std::move(hostPolicy))
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

    auto sourceConfig = statement.sourceConfig;
    const auto host = [&]
    {
        if (auto it = sourceConfig.find("host"); it != sourceConfig.end())
        {
            const auto host = it->second;
            sourceConfig.erase(it);
            return host;
        }
        return std::visit(
            Overloaded{
                [](const DefaultHost& defaultHost) -> std::string { return defaultHost.hostName; },
                [](const RequireHostConfig&) -> std::string
                { throw InvalidStatement("Could not handle source statement. `SOURCE`.`HOST` was not set"); }},
            hostPolicy);
    }();

    if (const auto created
        = sourceCatalog->addPhysicalSource(*logicalSource, statement.sourceType, host, sourceConfig, statement.parserConfig))
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
            return DropLogicalSourceStatementResult{.dropped = statement.source, .schema = *logical->getSchema()};
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

SinkStatementHandler::SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog, HostPolicy hostPolicy)
    : sinkCatalog(sinkCatalog), hostPolicy(std::move(hostPolicy))
{
}

std::expected<CreateSinkStatementResult, Exception> SinkStatementHandler::operator()(const CreateSinkStatement& statement)
{
    auto sinkConfig = statement.sinkConfig;
    const auto host = [&]
    {
        if (auto it = sinkConfig.find("host"); it != sinkConfig.end())
        {
            const auto host = it->second;
            sinkConfig.erase(it);
            return host;
        }
        return std::visit(
            Overloaded{
                [](const DefaultHost& defaultHost) -> std::string { return defaultHost.hostName; },
                [](const RequireHostConfig&) -> std::string
                { throw InvalidStatement("Could not handle sink statement. `SINK`.`HOST` was not set"); }},
            hostPolicy);
    }();

    if (const auto created = sinkCatalog->addSinkDescriptor(statement.name, statement.schema, statement.sinkType, host, sinkConfig))
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
    const auto sink = sinkCatalog->getSinkDescriptor(statement.name);
    if (not sink.has_value())
    {
        throw UnknownSinkName("Cannot remove unknown sink: {}", statement.name);
    }
    if (sinkCatalog->removeSinkDescriptor(sink.value()))
    {
        return DropSinkStatementResult{sink.value()};
    }
    return std::unexpected{UnknownSinkName(statement.name)};
}

QueryStatementHandler::QueryStatementHandler(SharedPtr<QueryManager> queryManager, SharedPtr<const LegacyOptimizer> optimizer)
    : queryManager(std::move(queryManager)), optimizer(std::move(optimizer))
{
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

    return stopResult;
}

std::expected<ExplainQueryStatementResult, Exception> QueryStatementHandler::operator()(const ExplainQueryStatement& statement)
{
    CPPTRACE_TRY
    {
        std::stringstream explainMessage;
        fmt::println(explainMessage, "Query:\n{}", statement.plan.getOriginalSql());
        fmt::println(explainMessage, "Initial Logical Plan:\n{}", statement.plan);

        const auto distributedPlan = optimizer->optimize(statement.plan);

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
        auto distributedPlan = optimizer->optimize(statement.plan);

        if (statement.id)
        {
            distributedPlan.setQueryId(DistributedQueryId(*statement.id));
        }

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
            .transform([](auto query) { return QueryStatementResult{std::move(query)}; });
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
        statement.capacity.value_or(INFINITE_CAPACITY),
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
                              return QueryStatusFailed(
                                  "Could not fetch status for query: {}",
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

    const auto statusOpt = queryManager->status(statement.id.value());
    if (statusOpt)
    {
        return ShowQueriesStatementResult{
            std::unordered_map<DistributedQueryId, DistributedQueryStatus>{{statement.id.value(), statusOpt.value()}}};
    }
    return std::unexpected(QueryStatusFailed("Could not retrieve query status for some queries: ", fmt::join(statusOpt.error(), "\n")));
}
}
