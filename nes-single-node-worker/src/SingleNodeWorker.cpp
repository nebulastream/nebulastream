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

#include <memory>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <StatisticPrinter.hpp>

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : compiler(std::make_unique<QueryCompilation::QueryCompiler>(
          configuration.workerConfiguration.queryCompiler, *QueryCompilation::Phases::DefaultPhaseFactory::create()))
    , listener(std::make_shared<Runtime::PrintingStatisticListener>(
          fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid())))
    , nodeEngine(Runtime::NodeEngineBuilder(configuration.workerConfiguration, listener, listener).build())
    , bufferSize(configuration.workerConfiguration.bufferSizeInBytes.getValue())
{
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

QueryId SingleNodeWorker::registerQuery(const std::shared_ptr<DecomposedQueryPlan>& plan)
{
    try
    {
        auto logicalQueryPlan = std::make_shared<DecomposedQueryPlan>(QueryId(queryIdCounter++), "", plan->getRootOperators());

        listener->onEvent(Runtime::SubmitQuerySystemEvent{logicalQueryPlan->getQueryId(), plan->toString()});

        auto request = QueryCompilation::QueryCompilationRequest::create(logicalQueryPlan, bufferSize);

        return nodeEngine->registerExecutableQueryPlan(compiler->compileQuery(request));
    }
    catch (Exception& e)
    {
        tryLogCurrentException();
        return INVALID_QUERY_ID;
    }
}

void SingleNodeWorker::startQuery(QueryId queryId)
{
    nodeEngine->startQuery(queryId);
}

void SingleNodeWorker::stopQuery(QueryId queryId, Runtime::QueryTerminationType type)
{
    nodeEngine->stopQuery(queryId, type);
}

void SingleNodeWorker::unregisterQuery(QueryId queryId)
{
    nodeEngine->unregisterQuery(queryId);
}

std::optional<Runtime::QuerySummary> SingleNodeWorker::getQuerySummary(QueryId queryId) const
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
        if (summary.currentStatus == Runtime::Execution::QueryStatus::Running)
        {
            INVARIANT(summary.runs.back().running.has_value(), "If query is running it should have a running timestamp");
            if (summary.runs.back().running.value() >= after)
            {
                status.activeQueries.emplace_back(summary.queryId, summary.runs.back().running.value());
            }
        }
        else if (summary.currentStatus == Runtime::Execution::QueryStatus::Stopped)
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

std::optional<Runtime::QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

}
