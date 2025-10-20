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
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <regex>
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
#include <Replay/ReplayStorage.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <cpptrace/from_current.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <NetworkOptions.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

extern void initNetworkServices(const std::string& connectionAddr, const NES::Host& host, const NES::NetworkOptions& options);

namespace NES
{

namespace
{
std::regex PlanFilePattern(R"(query_[0-9]+\.plan)");
}

SingleNodeWorker::~SingleNodeWorker()
{
    CheckpointManager::shutdown();
}
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, Host host)
    : listener(std::make_shared<CompositeStatisticListener>())
    , replayStatisticsCollector(std::make_shared<ReplayStatisticsCollector>())
    , configuration(configuration)
{
    {
        std::stringstream configStr;
        ConfigValuePrinter printer(configStr);
        SingleNodeWorkerConfiguration(configuration).accept(printer);
        NES_INFO("Starting SingleNodeWorker {} with configuration:\n{}", host.getRawValue(), configStr.str());
    }
    const auto& checkpointConfig = configuration.workerConfiguration.checkpointConfiguration;
    const auto checkpointDir = std::filesystem::path(checkpointConfig.checkpointDirectory.getValue());
    const auto checkpointInterval = std::chrono::milliseconds(checkpointConfig.checkpointIntervalMs.getValue());
    const auto recoverFromCheckpoint = checkpointConfig.recoverFromCheckpoint.getValue();

    CheckpointManager::initialize(checkpointDir, checkpointInterval, recoverFromCheckpoint);
    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", host.getRawValue(), std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }
    listener->addQueryEngineListener(copyPtr(replayStatisticsCollector));

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(host);

    optimizer = std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryOptimization);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(configuration.workerConfiguration.defaultQueryExecution);

    if (!configuration.data.getValue().empty())
    {
        const auto& networkConfig = configuration.workerConfiguration.network;
        initNetworkServices(
            configuration.data.getValue(),
            host,
            NetworkOptions{
                .senderQueueSize = static_cast<uint32_t>(networkConfig.senderQueueSize.getValue()),
                .maxPendingAcks = static_cast<uint32_t>(networkConfig.maxPendingAcks.getValue()),
                .receiverQueueSize = static_cast<uint32_t>(networkConfig.receiverQueueSize.getValue()),
                .senderIOThreads = static_cast<uint32_t>(networkConfig.senderIOThreads.getValue()),
                .receiverIOThreads = static_cast<uint32_t>(networkConfig.receiverIOThreads.getValue()),
            });
    }

    if (recoverFromCheckpoint)
    {
        recoverLatestCheckpoint(checkpointDir);
    }
}

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        /// Check if the plan already has a local query ID, generate one if needed
        /// but preserve the distributed query ID if present
        if (plan.getQueryId().getLocalQueryId() == INVALID_LOCAL_QUERY_ID)
        {
            auto localId = LocalQueryId(generateUUID());
            if (plan.getQueryId().isDistributed())
            {
                plan.setQueryId(QueryId::create(localId, plan.getQueryId().getDistributedQueryId()));
            }
            else
            {
                plan.setQueryId(QueryId::createLocal(localId));
            }
        }

        const LogContext context("queryId", plan.getQueryId());
        auto serializedPlanProto = QueryPlanSerializationUtil::serializeQueryPlan(plan);
        std::string serializedPlan;
        serializedPlanProto.SerializeToString(&serializedPlan);
        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        request->dumpCompilationResult = dumpMode;
        request->compilationCacheEnabled = configuration.workerConfiguration.enableCompilationCache.getValue();
        request->compilationCacheDir = configuration.workerConfiguration.compilationCacheDir.getValue();
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        replayStatisticsCollector->registerCompiledQueryPlan(plan.getQueryId(), *result);
        nodeEngine->registerCompiledQueryPlan(plan.getQueryId(), std::move(result));
        const auto queryId = plan.getQueryId();
        const auto callbackId = fmt::format("query_plan_{}", queryId.getRawValue());
        const auto checkpointIntervalMs = configuration.workerConfiguration.checkpointConfiguration.checkpointIntervalMs.getValue();
        if (checkpointIntervalMs > 0)
        {
            auto persistPlan = [planBytes = serializedPlan, queryId]() mutable
            {
                const auto fileName = fmt::format("query_{}.plan", queryId.getRawValue());
                CheckpointManager::persistFile(fileName, planBytes);
            };
            persistPlan();
            CheckpointManager::registerCallback(callbackId, std::move(persistPlan));
        }
        return queryId;
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
        replayStatisticsCollector->unregisterQuery(queryId);
        nodeEngine->unregisterQuery(queryId);
        CheckpointManager::unregisterCallback(fmt::format("query_plan_{}", queryId.getRawValue()));
        CheckpointManager::unregisterCallback(fmt::format("query_state_{}", queryId.getRawValue()));
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<LocalQueryStatus, Exception> SingleNodeWorker::getQueryStatus(QueryId queryId) const noexcept
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
            case QueryState::Registered:
                /// Ignore these for the worker status
                break;
            case QueryState::Started:
                ++status.replayMetrics.activeQueryCount;
                INVARIANT(metrics.start.has_value(), "If query is started, it should have a start timestamp");
                if (metrics.start.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, std::nullopt);
                }
                break;
            case QueryState::Running: {
                ++status.replayMetrics.activeQueryCount;
                INVARIANT(metrics.running.has_value(), "If query is running, it should have a running timestamp");
                if (metrics.running.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, metrics.running.value());
                }
                break;
            }
            case QueryState::Stopped: {
                INVARIANT(metrics.running.has_value(), "If query is stopped, it should have a running timestamp");
                INVARIANT(metrics.stop.has_value(), "If query is stopped, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
            case QueryState::Failed: {
                INVARIANT(metrics.stop.has_value(), "If query has failed, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
        }
    }
    status.replayMetrics.recordingStatuses = Replay::getRecordingRuntimeStatuses();
    for (const auto& recordingStatus : status.replayMetrics.recordingStatuses)
    {
        status.replayMetrics.recordingStorageBytes += recordingStatus.storageBytes;
    }
    status.replayMetrics.recordingFileCount = status.replayMetrics.recordingStatuses.size();
    status.replayMetrics.replayReadBytes = Replay::getReplayReadBytes();
    status.replayMetrics.operatorStatistics = replayStatisticsCollector->snapshot();
    return status;
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

void SingleNodeWorker::recoverLatestCheckpoint(const std::filesystem::path& checkpointDir)
{
    try
    {
        if (!std::filesystem::exists(checkpointDir))
        {
            NES_INFO("Checkpoint directory {} does not exist, skipping recovery", checkpointDir.string());
            return;
        }
        if (!std::filesystem::is_directory(checkpointDir))
        {
            NES_WARNING("Checkpoint path {} is not a directory, skipping recovery", checkpointDir.string());
            return;
        }

        std::optional<std::filesystem::directory_entry> newestEntry;
        std::error_code ec;
        for (std::filesystem::directory_iterator it(checkpointDir, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
        {
            const auto& entry = *it;
            if (!entry.is_regular_file())
            {
                continue;
            }
            const auto& filename = entry.path().filename().string();
            if (!std::regex_match(filename, PlanFilePattern))
            {
                continue;
            }
            if (!newestEntry || entry.last_write_time() > newestEntry->last_write_time())
            {
                newestEntry = entry;
            }
        }
        if (ec)
        {
            NES_WARNING("Failed to iterate checkpoint directory {}: {}", checkpointDir.string(), ec.message());
            return;
        }

        if (!newestEntry)
        {
            NES_INFO("No checkpoint plan files found in {}", checkpointDir.string());
            return;
        }

        auto serializedPlanOpt = loadSerializedPlanFromFile(newestEntry->path());
        if (!serializedPlanOpt)
        {
            NES_WARNING("Latest checkpoint {} does not contain a valid serialized plan", newestEntry->path().string());
            return;
        }

        SerializableQueryPlan proto;
        if (!proto.ParseFromString(*serializedPlanOpt))
        {
            NES_WARNING("Failed to parse serialized plan from {}", newestEntry->path().string());
            return;
        }

        auto plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
        auto registerResult = registerQuery(plan);
        if (!registerResult)
        {
            NES_WARNING("Failed to register recovered query from {}: {}", newestEntry->path().string(), registerResult.error().what());
            return;
        }
        NES_INFO("Recovered query {} from checkpoint {}", registerResult.value(), newestEntry->path().string());
        if (auto startResult = startQuery(registerResult.value()); !startResult)
        {
            NES_WARNING("Failed to start recovered query {}: {}", registerResult.value(), startResult.error().what());
        }
    }
    catch (const Exception& ex)
    {
        NES_WARNING("Exception during checkpoint recovery: {}", ex.what());
    }
    catch (const std::exception& stdex)
    {
        NES_WARNING("Std exception during checkpoint recovery: {}", stdex.what());
    }
}

std::optional<std::string> SingleNodeWorker::loadSerializedPlanFromFile(const std::filesystem::path& planFilePath)
{
    return CheckpointManager::loadFile(planFilePath);
}

}
