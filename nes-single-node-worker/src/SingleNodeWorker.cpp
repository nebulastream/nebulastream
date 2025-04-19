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

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
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
    , listener(std::make_shared<PrintingStatisticListener>(
          fmt::format("nes-stats-{:%H:%M:%S}-{}.txt", std::chrono::system_clock::now(), ::getpid())))
    , nodeEngine(NodeEngineBuilder(configuration.workerConfiguration, listener, listener).build())
    , bufferSize(configuration.workerConfiguration.bufferSizeInBytes.getValue())
    , optimizer(std::make_unique<Optimizer::QueryOptimizer>(configuration.workerConfiguration.queryOptimizer))
    , compiler(std::make_unique<QueryCompilation::QueryCompiler>())
{
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

QueryId SingleNodeWorker::registerQuery(LogicalPlan plan)
{
    try
    {
        auto logicalQueryPlan
            = std::make_shared<DecomposedQueryPlan>(QueryId(queryIdCounter++), INITIAL<WorkerId>, plan->getRootOperators());

        listener->onEvent(SubmitQuerySystemEvent{logicalQueryPlan->getQueryId(), plan->toString()});

        auto request = std::make_unique<QueryCompilationRequest>(logicalQueryPlan, bufferSize);

        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(std::move(queryPlan));

        return nodeEngine->registerCompiledQueryPlan(compiler->compileQuery(std::move(request)));
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

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

}
