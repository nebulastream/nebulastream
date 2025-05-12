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

#include <SingleNodeWorker.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <utility>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticPrinter.hpp>

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : listener(std::make_shared<PrintingStatisticListener>(
          fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid())))
    , nodeEngine(NodeEngineBuilder(configuration.workerConfiguration, listener, listener).build())
    , bufferSize(configuration.workerConfiguration.bufferSizeInBytes.getValue())
    , optimizer(std::make_unique<QueryOptimizer>(configuration.workerConfiguration.queryOptimizer))
    , compiler(std::make_unique<QueryCompilation::QueryCompiler>())
{
    if (configuration.workerConfiguration.bufferSizeInBytes.getValue()
        < configuration.workerConfiguration.queryOptimizer.operatorBufferSize.getValue())
    {
        throw InvalidConfigParameter(
            "Currently, we require the bufferSizeInBytes {} to be at least the operatorBufferSize {}",
            configuration.workerConfiguration.bufferSizeInBytes.getValue(),
            configuration.workerConfiguration.queryOptimizer.operatorBufferSize.getValue());
    }
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) const
{
    try
    {
        plan.setQueryId(QueryId(queryIdCounter++));
        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        auto result = compiler->compileQuery(std::move(request));
        return nodeEngine->registerCompiledQueryPlan(std::move(result));
    }
    catch (Exception& e)
    {
        tryLogCurrentException();
        return std::unexpected(e);
    }
}

void SingleNodeWorker::startQuery(QueryId queryId)
{
    nodeEngine->startQuery(queryId);
}

void SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type)
{
    nodeEngine->stopQuery(queryId, type);
}

void SingleNodeWorker::unregisterQuery(QueryId queryId)
{
    nodeEngine->unregisterQuery(queryId);
}

std::optional<QuerySummary> SingleNodeWorker::getQuerySummary(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getQuerySummary(queryId);
}
WorkerStatus SingleNodeWorker::getWorkerStatus(std::chrono::system_clock::time_point after) const
{
    auto summaries = nodeEngine->getQueryLog()->getSummary();
    WorkerStatus status;
    for (auto& summary : summaries)
    {
        INVARIANT(!summary.runs.empty(), "Query should at least contain a single run");
        if (summary.currentStatus == QueryStatus::Running)
        {
            INVARIANT(summary.runs.back().running.has_value(), "If query is running it should have a running timestamp");
            if (summary.runs.back().running.value() >= after)
            {
                status.activeQueries.emplace_back(summary.queryId, summary.runs.back().running.value());
            }
        }
        else if (summary.currentStatus == QueryStatus::Stopped)
        {
            INVARIANT(summary.runs.back().running.has_value(), "If query is stopped it should have a running timestamp");
            INVARIANT(summary.runs.back().stop.has_value(), "If query is stopped it should have a stopped timestamp");
            if (summary.runs.back().stop.value() >= after)
            {
                status.terminatedQueries.emplace_back(
                    summary.queryId,
                    summary.runs.back().running.value(),
                    summary.runs.back().stop.value(),
                    std::move(summary.runs.back().error));
            }
        }
    }
    return status;
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

}
