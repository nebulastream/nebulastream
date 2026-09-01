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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <unistd.h>
#include <Configurations/ConfigValuePrinter.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>

#include <Statistics/DefaultStatisticStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <BufferStatisticListener.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <NetworkOptions.hpp>
#include <QueryCompiler.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <TaskStatisticListener.hpp>
#include <WorkerStatus.hpp>

extern void initNetworkServices(const std::string& connectionAddr, const NES::Host& host, const NES::NetworkOptions& options);

namespace NES
{

namespace
{
/// Number of statistics rows each feed buffers before further rows are dropped. One row per flush interval
/// is a low rate for the buffer feed, so this is generous: a query reading it can start minutes after the
/// worker did.
constexpr size_t TASK_STATISTICS_FEED_CAPACITY = 8192;
constexpr size_t BUFFER_STATISTICS_FEED_CAPACITY = 8192;
}

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, const Host& host)
    : listener(std::make_shared<CompositeStatisticListener>()), configuration(configuration)
{
    {
        std::stringstream configStr;
        ConfigValuePrinter printer(configStr);
        SingleNodeWorkerConfiguration(configuration).accept(printer);
        NES_INFO("Starting SingleNodeWorker {} with configuration:\n{}", host.getRawValue(), configStr.str());
    }
    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", host.getRawValue(), std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }

    if (configuration.enableTaskStatistics.getValue())
    {
        auto taskStatisticListener = std::make_shared<TaskStatisticListener>(host, TASK_STATISTICS_FEED_CAPACITY);
        taskStatisticListener->start();
        listener->addListener(taskStatisticListener);
    }

    if (configuration.enableBufferStatistics.getValue())
    {
        bufferListener = std::make_shared<BufferStatisticListener>(
            host, BUFFER_STATISTICS_FEED_CAPACITY, std::chrono::milliseconds{configuration.bufferStatisticsIntervalMs.getValue()});
        bufferListener->start();
    }

    /// All listeners have to be in place before the engine exists: CompositeStatisticListener does not
    /// synchronize its listener lists against the worker threads that read them, and the BufferManager gets
    /// its listener handed over at construction time.
    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener), bufferListener).build(host);
    /// One statistic store per worker: statistic build queries write into it, probe queries read from it.
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(
        configuration.workerConfiguration.defaultQueryExecution, std::make_shared<DefaultStatisticStore>());

    if (!configuration.dataAddress.getValue().empty())
    {
        const auto& networkConfig = configuration.workerConfiguration.network;
        initNetworkServices(
            configuration.dataAddress.getValue(),
            host,
            NetworkOptions{
                .senderQueueSize = static_cast<uint32_t>(networkConfig.senderQueueSize.getValue()),
                .maxPendingAcks = static_cast<uint32_t>(networkConfig.maxPendingAcks.getValue()),
                .receiverQueueSize = static_cast<uint32_t>(networkConfig.receiverQueueSize.getValue()),
                .senderIOThreads = static_cast<uint32_t>(networkConfig.senderIOThreads.getValue()),
                .receiverIOThreads = static_cast<uint32_t>(networkConfig.receiverIOThreads.getValue()),
            });
    }
}

std::expected<QueryId, Exception> SingleNodeWorker::startQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        INVARIANT(plan.getQueryId() != INVALID_QUERY_ID, "Plan must have a valid QueryId");

        const LogContext context("queryId", plan.getQueryId());

        const auto planStr = explain(plan, ExplainVerbosity::Debug);
        NES_INFO("Registering query {} with plan:\n{}", plan.getQueryId(), planStr);
        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), planStr});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(plan);
        request->dumpCompilationResult = dumpMode;
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        nodeEngine->startQuery(plan.getQueryId(), std::move(result));
        return plan.getQueryId();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<LocalQueryStatusSnapshot, Exception> SingleNodeWorker::getQueryStatus(QueryId queryId) const noexcept
{
    CPPTRACE_TRY
    {
        auto status = nodeEngine->getQueryLog()->getQueryStatus(queryId);
        if (not status.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return status.value();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

WorkerStatus SingleNodeWorker::getWorkerStatus(std::chrono::system_clock::time_point after) const
{
    const std::chrono::system_clock::time_point until = std::chrono::system_clock::now();
    const auto summaries = nodeEngine->getQueryLog()->getStatus();
    WorkerStatus status;
    status.after = after;
    status.until = until;
    for (const auto& [queryId, state, metrics] : summaries)
    {
        switch (state)
        {
            case QueryStatus::Registered:
                /// Ignore these for the worker status
                break;
            case QueryStatus::Started:
                INVARIANT(metrics.start.has_value(), "If query is started, it should have a start timestamp");
                if (metrics.start.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, std::nullopt);
                }
                break;
            case QueryStatus::Running: {
                INVARIANT(metrics.running.has_value(), "If query is running, it should have a running timestamp");
                if (metrics.running.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, metrics.running.value());
                }
                break;
            }
            case QueryStatus::Stopped: {
                INVARIANT(metrics.running.has_value(), "If query is stopped, it should have a running timestamp");
                INVARIANT(metrics.stop.has_value(), "If query is stopped, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
            case QueryStatus::Failed: {
                INVARIANT(metrics.stop.has_value(), "If query has failed, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
        }
    }
    return status;
}

}
