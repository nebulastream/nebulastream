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

#include <StatementHandler.hpp>

#include <algorithm>
#include <expected>
#include <memory>
#include <mutex>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <ErrorHandling.hpp>

#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <cpptrace/from_current.hpp>
#include <LegacyOptimizer.hpp>

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
    if (const auto created
        = sourceCatalog->addPhysicalSource(statement.attachedTo, statement.sourceType, statement.sourceConfig, statement.parserConfig))
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
        if (const auto foundSources = sourceCatalog->getPhysicalSources(*statement.logicalSource))
        {
            return ShowPhysicalSourcesStatementResult{*foundSources | std::ranges::to<std::vector>()};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (statement.logicalSource and statement.id)
    {
        if (const auto foundSources = sourceCatalog->getPhysicalSources(*statement.logicalSource))
        {
            return ShowPhysicalSourcesStatementResult{
                foundSources.value()
                | std::views::filter([statement](const auto& source)
                                     { return source.getPhysicalSourceId() == PhysicalSourceId{statement.id.value()}; })
                | std::ranges::to<std::vector>()};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    return ShowPhysicalSourcesStatementResult{
        sourceCatalog->getLogicalToPhysicalSourceMapping() | std::views::transform([](auto& pair) { return pair.second; })
        | std::views::join | std::ranges::to<std::vector>()};
}

std::expected<DropLogicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropLogicalSourceStatement& statement)
{
    if (sourceCatalog->removeLogicalSource(statement.source))
    {
        return DropLogicalSourceStatementResult{statement.source};
    }
    return std::unexpected{UnknownSourceName(statement.source.getLogicalSourceName())};
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
    if (const auto created = sinkCatalog->addSinkDescriptor(statement.name, statement.schema, statement.sinkType, statement.sinkConfig))
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

QueryStatementHandler::QueryStatementHandler(
    const std::shared_ptr<QueryManager>& queryManager, const std::shared_ptr<LegacyOptimizer>& optimizer)
    : queryManager(queryManager), optimizer(optimizer)
{
}

std::expected<DropQueryStatementResult, Exception> QueryStatementHandler::operator()(const DropQueryStatement& statement)
{
    const std::unique_lock lock(mutex);
    std::erase(runningQueries, statement.id);
    return queryManager->stop(statement.id)
        .and_then([&statement, this] { return queryManager->unregister(statement.id); })
        .transform([&statement] { return DropQueryStatementResult{statement.id}; });
}

std::expected<QueryStatementResult, Exception> QueryStatementHandler::operator()(const QueryStatement& statement)
{
    const std::unique_lock lock(mutex);
    CPPTRACE_TRY
    {
        const auto optimizedPlan = optimizer->optimize(statement);
        const auto id = queryManager->registerQuery(optimizedPlan);
        return id.and_then([this](const auto& queryId) { return queryManager->start(queryId); })
            .transform(
                [&id, this]
                {
                    runningQueries.push_back(id.value());
                    return QueryStatementResult{id.value()};
                });
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<ShowQueriesStatementResult, Exception> QueryStatementHandler::operator()(const ShowQueriesStatement& statement)
{
    const std::unique_lock lock(mutex);
    if (not statement.id.has_value())
    {
        auto allQueryState = runningQueries | std::views::transform([this](auto queryId) { return queryManager->status(queryId); })
            | std::views::filter([](const std::expected<LocalQueryStatus, Exception>& statusResult) { return statusResult.has_value(); })
            | std::views::transform([](const auto& statusResult) { return statusResult.value(); }) | std::ranges::to<std::vector>();
        auto newRunningQueries = allQueryState | std::views::transform([](const auto& querySummary) { return querySummary.queryId; })
            | std::ranges::to<std::vector>();
        runningQueries = newRunningQueries;
        return ShowQueriesStatementResult{
            allQueryState
            | std::views::transform([](const auto& querySummary) { return std::make_pair(querySummary.queryId, querySummary); })
            | std::ranges::to<std::unordered_map<QueryId, LocalQueryStatus>>()};
    }
    if (const auto statusOpt = queryManager->status(statement.id.value()); statusOpt.has_value())
    {
        return ShowQueriesStatementResult{std::unordered_map<QueryId, LocalQueryStatus>{{statement.id.value(), statusOpt.value()}}};
    }
    return ShowQueriesStatementResult{.queries = {}};
}

std::vector<QueryId> QueryStatementHandler::getRunningQueries() const
{
    return runningQueries;
}

}
