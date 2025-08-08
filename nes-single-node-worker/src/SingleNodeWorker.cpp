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

#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticPrinter.hpp>

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration)
    : listener(
          std::make_shared<PrintingStatisticListener>(
              fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid())))
    , nodeEngine(NodeEngineBuilder(configuration.workerConfiguration, Util::copyPtr(listener), Util::copyPtr(listener)).build())
    , optimizer(std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryExecution))
    , compiler(std::make_unique<QueryCompilation::QueryCompiler>())
    , configuration(configuration)
{
    if (configuration.workerConfiguration.bufferSizeInBytes.getValue()
        < configuration.workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue())
    {
        throw InvalidConfigParameter(
            "Currently, we require the bufferSizeInBytes {} to be at least the operatorBufferSize {}",
            configuration.workerConfiguration.bufferSizeInBytes.getValue(),
            configuration.workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue());
    }
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        plan.setQueryId(QueryId(queryIdCounter++));
        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        request->dumpCompilationResult = configuration.workerConfiguration.dumpQueryCompilationIntermediateRepresentations.getValue();
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successfull query compilation or exception, but got nothing");
        return nodeEngine->registerCompiledQueryPlan(std::move(result));
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::startQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->startQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId, type);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::unregisterQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->unregisterQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::optional<QuerySummary> SingleNodeWorker::getQuerySummary(QueryId queryId) const
{
    CPPTRACE_TRY
    {
        auto summary = nodeEngine->getQueryLog()->getQuerySummary(queryId);
        if (not summary.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return summary.value();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

WorkerStatus SingleNodeWorker::getWorkerStatus(std::chrono::system_clock::time_point after) const
{
    auto summaries = nodeEngine->getQueryLog()->getSummary();
    WorkerStatus status;
    for (auto& summary : summaries)
    {
        if (summary.state == QueryState::Running)
        {
            INVARIANT(summary.metrics.running.has_value(), "If query is running, it should have a running timestamp");
            if (summary.metrics.running.value() >= after)
            {
                status.activeQueries.emplace_back(summary.queryId, summary.metrics.running.value());
            }
        }
        else if (summary.state == QueryState::Stopped)
        {
            INVARIANT(summary.metrics.running.has_value(), "If query is stopped, it should have a running timestamp");
            INVARIANT(summary.metrics.running.has_value(), "If query is stopped, it should have a stopped timestamp");
            if (summary.metrics.stop.value() >= after)
            {
                status.terminatedQueries.emplace_back(
                    summary.queryId, summary.metrics.running.value(), summary.metrics.stop.value(), std::move(summary.metrics.error));
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
