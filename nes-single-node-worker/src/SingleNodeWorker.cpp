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
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <regex>
#include <utility>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <fmt/format.h>
#include <cpptrace/from_current.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

namespace
{
std::regex PlanFilePattern(R"(query_[0-9]+\.plan)");

std::optional<std::filesystem::path> resolveRecoveryDirectory(const CheckpointConfiguration& checkpointConfiguration)
{
    const auto recoveryValue = checkpointConfiguration.recoverCheckpointDirectory.getValue();
    if (recoveryValue.empty())
    {
        return std::nullopt;
    }

    return std::filesystem::path{recoveryValue};
}
}

SingleNodeWorker::~SingleNodeWorker()
{
    CheckpointManager::shutdown();
}
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration)
    : listener(std::make_shared<CompositeStatisticListener>()), configuration(configuration)
{
    const auto& checkpointConfig = configuration.workerConfiguration.checkpointConfiguration;
    const auto checkpointDir = std::filesystem::path(checkpointConfig.checkpointDirectory.getValue());
    const auto checkpointInterval = std::chrono::milliseconds(checkpointConfig.checkpointIntervalMs.getValue());
    const auto recoveryDir = resolveRecoveryDirectory(checkpointConfig);

    CheckpointManager::initialize(checkpointDir, checkpointInterval, recoveryDir.has_value());

    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("GoogleEventTrace_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build();

    optimizer = std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryExecution);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>();

    if (recoveryDir)
    {
        recoverLatestCheckpoint(*recoveryDir);
    }
}

/// This is a workaround to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        plan.setQueryId(QueryId(queryIdCounter++));
        auto serializedPlanProto = QueryPlanSerializationUtil::serializeQueryPlan(plan);
        std::string serializedPlan;
        serializedPlanProto.SerializeToString(&serializedPlan);
        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        request->dumpCompilationResult = configuration.workerConfiguration.dumpQueryCompilationIntermediateRepresentations.getValue();
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successfull query compilation or exception, but got nothing");
        const auto queryId = nodeEngine->registerCompiledQueryPlan(std::move(result));
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

        std::optional<std::filesystem::directory_entry> newestEntry;
        for (const auto& entry : std::filesystem::directory_iterator(checkpointDir))
        {
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
