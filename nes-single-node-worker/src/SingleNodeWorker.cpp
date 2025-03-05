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
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <StatisticPrinter.hpp>
#include "../../nes-query-engine/TaskStatisticsProcessor.hpp"

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : compiler(std::make_unique<QueryCompilation::QueryCompiler>(
          configuration.workerConfiguration.queryCompiler, *QueryCompilation::Phases::DefaultPhaseFactory::create()))
    , queryStatisticsListeners(std::vector<std::shared_ptr<Runtime::QueryEngineStatisticListener>>{
          std::make_shared<Runtime::PrintingStatisticListener>(
              configuration.workerConfiguration.queryEngineConfiguration.statisticsDir,
              configuration.workerConfiguration.queryEngineConfiguration.numberOfWorkerThreads),
          std::make_shared<Runtime::TaskStatisticsProcessor>()})
    , systemEventListeners(std::vector<std::shared_ptr<Runtime::SystemEventListener>>{std::make_shared<Runtime::PrintingStatisticListener>(
          configuration.workerConfiguration.queryEngineConfiguration.statisticsDir,
          configuration.workerConfiguration.queryEngineConfiguration.numberOfWorkerThreads)})
    , nodeEngine(Runtime::NodeEngineBuilder(configuration.workerConfiguration, systemEventListeners, queryStatisticsListeners).build())
    , bufferSize(configuration.workerConfiguration.bufferSizeInBytes.getValue())
{
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

QueryId
SingleNodeWorker::registerQuery(const std::shared_ptr<DecomposedQueryPlan>& plan, const double minThroughput, const double maxLatency)
{
    try
    {
        const auto logicalQueryPlan
            = std::make_shared<DecomposedQueryPlan>(QueryId(queryIdCounter++), INITIAL<WorkerId>, plan->getRootOperators());

        for (const auto& listener : systemEventListeners)
        {
            listener->onEvent(Runtime::SubmitQuerySystemEvent{logicalQueryPlan->getQueryId(), plan->toString(), minThroughput, maxLatency});
        }

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

std::optional<Runtime::QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

}
