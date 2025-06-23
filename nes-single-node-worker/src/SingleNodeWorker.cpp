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
#include <ThroughputListener.hpp>

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : bufferSize(configuration.workerConfiguration.bufferSizeInBytes.getValue())
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

    /// Writing the current throughput to the log
    auto callback = [](std::ofstream& file, const ThroughputListener::CallBackParams& callBackParams)
    {
        /// Helper function to format the given value in SI units
        auto formatValue = [](double value, const std::string_view suffix)
        {
            constexpr std::array<const char*, 5> units = {"", "k", "M", "G", "T"};
            int unitIndex = 0;

            while (value >= 1000 && unitIndex < 4)
            {
                value /= 1000;
                unitIndex++;
            }

            return fmt::format("{:.3f} {}{}", value, units[unitIndex], suffix);
        };

        const auto bytesPerSecondMessage = formatValue(callBackParams.throughputInBytesPerSec, "B/s");
        const auto tuplesPerSecondMessage = formatValue(callBackParams.throughputInTuplesPerSec, "Tup/s");
        const auto memoryConsumptionMessage = formatValue(callBackParams.memoryConsumption, "B");
        if (callBackParams.queryId.getRawValue() == 1)
        {
            std::cout << fmt::format(
                "Throughput for queryId {} in window {}-{} is {} / {} and memory consumption is {}\n",
                callBackParams.queryId,
                callBackParams.windowStart,
                callBackParams.windowEnd,
                bytesPerSecondMessage,
                tuplesPerSecondMessage,
                memoryConsumptionMessage);

            /// We need to flush the file now as the worker might be killed abruptly
            //file.flush();
            (void)file;
        }
    };
    const auto timeIntervalInMilliSeconds = configuration.workerConfiguration.throughputListenerTimeInterval.getValue();
    auto throughputListener = std::make_shared<ThroughputListener>(
        fmt::format("BenchmarkStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid()),
        timeIntervalInMilliSeconds,
        callback);

    const auto printStatisticListener = std::make_shared<PrintingStatisticListener>(
        fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid()));
    queryEngineStatisticsListener = {printStatisticListener, throughputListener};
    systemEventListener = printStatisticListener;
    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, systemEventListener, queryEngineStatisticsListener).build();
    throughputListener->setBufferManager(nodeEngine->getBufferManager());
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
        systemEventListener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
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

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

}
