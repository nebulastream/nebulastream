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
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>

namespace NES
{

/// TODO(#122): Refactor QueryCompilerConfiguration
static QueryCompilation::QueryCompilerOptionsPtr
createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration& queryCompilerConfiguration)
{
    auto queryCompilerType = queryCompilerConfiguration.queryCompilerType;
    auto queryCompilationOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();

    /// set compilation mode
    queryCompilationOptions->setCompilationStrategy(queryCompilerConfiguration.compilationStrategy);

    /// set pipelining strategy mode
    queryCompilationOptions->setPipeliningStrategy(queryCompilerConfiguration.pipeliningStrategy);

    /// set output buffer optimization level
    queryCompilationOptions->setOutputBufferOptimizationLevel(queryCompilerConfiguration.outputBufferOptimizationLevel);

    if (queryCompilerType == QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER
        && queryCompilerConfiguration.windowingStrategy == QueryCompilation::WindowingStrategy::LEGACY)
    {
        /// sets SLICING windowing strategy as the default if nautilus is active.
        NES_WARNING("The LEGACY window strategy is not supported by Nautilus. Switch to SLICING!")
        queryCompilationOptions->setWindowingStrategy(QueryCompilation::WindowingStrategy::SLICING);
    }
    else
    {
        /// sets the windowing strategy
        queryCompilationOptions->setWindowingStrategy(queryCompilerConfiguration.windowingStrategy);
    }

    /// sets the query compiler
    queryCompilationOptions->setQueryCompiler(queryCompilerConfiguration.queryCompilerType);

    /// set the dump mode
    queryCompilationOptions->setDumpMode(queryCompilerConfiguration.queryCompilerDumpMode);

    /// set nautilus backend
    queryCompilationOptions->setNautilusBackend(queryCompilerConfiguration.nautilusBackend);

    queryCompilationOptions->getHashJoinOptions()->setNumberOfPartitions(queryCompilerConfiguration.numberOfPartitions.getValue());
    queryCompilationOptions->getHashJoinOptions()->setPageSize(queryCompilerConfiguration.pageSize.getValue());
    queryCompilationOptions->getHashJoinOptions()->setPreAllocPageCnt(queryCompilerConfiguration.preAllocPageCnt.getValue());
    ///zero indicate that it has not been set in the yaml config
    if (queryCompilerConfiguration.maxHashTableSize.getValue() != 0)
    {
        queryCompilationOptions->getHashJoinOptions()->setTotalSizeForDataStructures(
            queryCompilerConfiguration.maxHashTableSize.getValue());
    }

    queryCompilationOptions->setStreamJoinStrategy(queryCompilerConfiguration.joinStrategy);

    queryCompilationOptions->setCUDASdkPath(queryCompilerConfiguration.cudaSdkPath.getValue());

    return queryCompilationOptions;
}

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : qc(std::make_unique<QueryCompilation::NautilusQueryCompiler>(
          createQueryCompilationOptions(configuration.queryCompilerConfiguration), QueryCompilation::Phases::DefaultPhaseFactory::create()))
    , nodeEngine(Runtime::NodeEngineBuilder(configuration.engineConfiguration).build())
{
}

/// TODO(#305): This is a hotfix to get again unique queryId after our ininital worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

QueryId SingleNodeWorker::registerQuery(DecomposedQueryPlanPtr plan)
{
    try
    {
        auto compilationResult
            = qc->compileQuery(QueryCompilation::QueryCompilationRequest::create(std::move(plan), nodeEngine), QueryId(queryIdCounter++));
        return nodeEngine->registerExecutableQueryPlan(compilationResult->getExecutableQueryPlan());
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

} /// namespace NES
