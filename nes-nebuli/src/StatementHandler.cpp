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

#include <cpptrace/from_current.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include "DistributedQueryId.hpp"
#include "Util/Pointers.hpp"

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
    if (const auto created = sourceCatalog->addPhysicalSource(
            statement.attachedTo, statement.sourceType, statement.workerId, statement.sourceConfig, statement.parserConfig))
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

WorkerStatementHandler::WorkerStatementHandler(const std::shared_ptr<WorkerCatalog>& workerCatalog) : workerCatalog{workerCatalog}
{
}

std::expected<CreateWorkerStatementResult, Exception> WorkerStatementHandler::operator()(const CreateWorkerStatement& statement)
{
    if (workerCatalog->addWorker(statement.host, statement.grpc, statement.capacity, statement.downstream))
    {
        return CreateWorkerStatementResult{};
    }
    return std::unexpected{WorkerAlreadyExists(statement.host)};
}

std::expected<ShowWorkersStatementResult, Exception> WorkerStatementHandler::operator()(const ShowWorkersStatement& statement) const
{
    if (statement.worker)
    {
        if (const auto foundWorker = workerCatalog->getWorker(*statement.worker))
        {
            return ShowWorkersStatementResult{std::vector{*foundWorker}};
        }
        return ShowWorkersStatementResult{{}};
    }
    return ShowWorkersStatementResult{workerCatalog->getAllWorkers()};
}

std::expected<DropWorkerStatementResult, Exception> WorkerStatementHandler::operator()(const DropWorkerStatement& statement)
{
    if (const auto removed = workerCatalog->removeWorker(statement.worker))
    {
        return DropWorkerStatementResult{std::move(removed).value()};
    }
    return std::unexpected{UnknownWorkerAddr(statement.worker)};
}

QueryStatementHandler::QueryStatementHandler(
    SharedPtr<QueryManager> queryManager,
    SharedPtr<SourceCatalog> sourceCatalog,
    SharedPtr<SinkCatalog> sinkCatalog,
    SharedPtr<WorkerCatalog> workerCatalog)
    : queryManager{copyPtr(queryManager)}
    , sourceCatalog{copyPtr(sourceCatalog)}
    , sinkCatalog{copyPtr(sinkCatalog)}
    , workerCatalog{copyPtr(workerCatalog)}
{
}

std::expected<DropQueryStatementResult, std::vector<Exception>> QueryStatementHandler::operator()(const DropQueryStatement& statement)
{
    const std::unique_lock lock(mutex);
    const auto it = std::ranges::find_if(runningQueries, [&statement](const Query& query) { return query.getId() == statement.id; });
    if (it == std::end(runningQueries))
    {
        return std::unexpected{std::vector{QueryNotFound("Query {} has already been dropped", statement.id)}};
    }

    auto nodeHandle = runningQueries.extract(it);
    Query queryToStop = std::move(nodeHandle.value());

    return queryManager->stop(queryToStop)
        .and_then([&queryToStop, this] { return queryManager->unregister(queryToStop); })
        .transform([&statement] { return DropQueryStatementResult{statement.id}; });
}

std::expected<QueryStatementResult, Exception> QueryStatementHandler::operator()(const QueryStatement& statement)
{
    const std::unique_lock lock(mutex);
    CPPTRACE_TRY
    {
        QueryPlanningContext context{
            .id = statement.getQueryId(),
            .sqlString = statement.getOriginalSql(),
            .sourceCatalog = copyPtr(sourceCatalog),
            .sinkCatalog = copyPtr(sinkCatalog),
            .workerCatalog = copyPtr(workerCatalog),
        };

        // Plan the query using QueryPlanner - statement is a LogicalPlan
        auto boundPlan = PlanStage::BoundLogicalPlan{statement};
        const auto finalizedPlan = QueryPlanner::with(context).plan(std::move(boundPlan));

        auto queryHandle = queryManager->registerQuery(finalizedPlan);
        if (!queryHandle.has_value())
        {
            return std::unexpected{queryHandle.error()};
        }

        Query handle = std::move(queryHandle).value();
        const auto id = handle.getId();
        if (auto startResult = queryManager->start(handle); !startResult.has_value())
        {
            return std::unexpected{startResult.error()};
        }

        runningQueries.emplace(std::move(handle));
        return QueryStatementResult{id};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<ShowQueriesStatementResult, std::vector<Exception>> QueryStatementHandler::operator()(const ShowQueriesStatement& statement)
{
    const std::unique_lock lock(mutex);
    if (not statement.id.has_value())
    {
        std::unordered_map<DistributedQueryId, DistributedQueryStatus> validQueries;
        std::vector<Exception> allErrors;

        for (const auto& query : runningQueries)
        {
            if (auto statusResult = queryManager->status(query); statusResult.has_value())
            {
                validQueries[query.getId()] = statusResult.value();
            }
            else
            {
                allErrors.insert(allErrors.end(), statusResult.error().begin(), statusResult.error().end());
            }
        }

        if (not allErrors.empty())
        {
            return std::unexpected{allErrors};
        }

        return ShowQueriesStatementResult{validQueries};
    }

    const auto it
        = std::ranges::find_if(runningQueries, [&statement](const Query& query) { return query.getId() == statement.id.value(); });

    if (it == std::end(runningQueries))
    {
        return std::unexpected{std::vector{QueryNotFound("Query {} not found", statement.id.value())}};
    }

    auto statusResult = queryManager->status(*it);
    if (statusResult.has_value())
    {
        return ShowQueriesStatementResult{
            std::unordered_map<DistributedQueryId, DistributedQueryStatus>{{statement.id.value(), statusResult.value()}}};
    }
    return std::unexpected(statusResult.error());
}

auto QueryStatementHandler::getRunningQueries() const
{
    return runningQueries | std::views::all;
}

}
