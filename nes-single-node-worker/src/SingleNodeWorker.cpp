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

#include <expected>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Async/HandoffChannel.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> /// NOLINT(misc-include-cleaner) Public fmt formatter specializations for the strong identifiers used by fmt::format.
#include <Listeners/QueryLog.hpp> /// NOLINT(misc-include-cleaner) Public QueryLog declaration required by NodeEngine query-log API use.
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <Util/DumpMode.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade declares LogContext.
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <Util/UUID.hpp>
#include <cpptrace/from_current.hpp> /// NOLINT(misc-include-cleaner) Public header declares CPPTRACE_TRY and CPPTRACE_CATCH.
#include <fmt/format.h>
#include <CompiledQueryPlan.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <NetworkOptions.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

extern void initNetworkServices(const std::string& connectionAddr, const NES::Host& host, const NES::NetworkOptions& options);

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, const Host& host)
    : listener(std::make_shared<CompositeStatisticListener>()), configuration(configuration)
{
    NES_INFO("Starting SingleNodeWorker {}", host.getRawValue());
    if (configuration.enableGoogleEventTrace)
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", host.getRawValue(), std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(host);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(configuration.workerConfiguration.defaultQueryExecution);

    if (!configuration.dataAddress.empty())
    {
        const auto& networkConfig = configuration.workerConfiguration.network;
        initNetworkServices(
            configuration.dataAddress,
            host,
            NetworkOptions{
                .senderQueueSize = static_cast<uint32_t>(networkConfig.senderQueueSize),
                .maxPendingAcks = static_cast<uint32_t>(networkConfig.maxPendingAcks),
                .receiverQueueSize = static_cast<uint32_t>(networkConfig.receiverQueueSize),
                .senderIOThreads = static_cast<uint32_t>(networkConfig.senderIOThreads),
                .receiverIOThreads = static_cast<uint32_t>(networkConfig.receiverIOThreads),
            });
    }
}

std::expected<QueryId, Exception> SingleNodeWorker::startQuery(LogicalPlan plan) noexcept
{
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_TRY is declared by public cpptrace/from_current.hpp.
    CPPTRACE_TRY
    {
        auto result = startQueries(std::vector<LogicalPlan>{std::move(plan)});
        if (!result)
        {
            return std::unexpected(result.error());
        }
        return result->front();
    }
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_CATCH is declared by public cpptrace/from_current.hpp.
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<std::vector<QueryId>, Exception> SingleNodeWorker::startQueries(std::vector<LogicalPlan> plans) noexcept
{
    std::vector<QueryId> successfullyStarted;
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_TRY is declared by public cpptrace/from_current.hpp.
    CPPTRACE_TRY
    {
        /// Clone the entire batch before compiling: logical plans may share mutable runtime
        /// configuration with their callers, but every execution needs fresh pointees.
        /// Separate contexts retain cross-plan identity without reusing original objects.
        const ReflectionContext serializationContext{std::make_shared<SharedPointerSerializationState>()};
        const auto sharedObjects = std::make_shared<SharedPointerDeserializationState>();
        const ReflectionContext executionContext{sharedObjects};
        std::vector<LogicalPlan> executionPlans;
        executionPlans.reserve(plans.size());
        for (const auto& plan : plans)
        {
            const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(plan, serializationContext);
            executionPlans.emplace_back(QueryPlanSerializationUtil::deserializeQueryPlan(serialized, executionContext));
        }

        std::vector<std::unique_ptr<CompiledQueryPlan>> compiledPlans;
        std::vector<QueryId> queryIds;
        compiledPlans.reserve(plans.size());
        queryIds.reserve(plans.size());
        for (auto& plan : executionPlans)
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

            /// NOLINTNEXTLINE(misc-include-cleaner) LogContext is declared by public Util/Logger/Logger.hpp.
            const LogContext context("queryId", plan.getQueryId());

            const auto planStr = explain(plan, ExplainVerbosity::Debug);
            NES_INFO("Registering query {} with plan:\n{}", plan.getQueryId(), planStr);
            listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), planStr});
            const DumpMode dumpMode(configuration.workerConfiguration.dumpQueryCompilationIR, configuration.workerConfiguration.dumpGraph);
            auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(plan);
            request->dumpCompilationResult = dumpMode;
            auto result = compiler->compileQuery(std::move(request));
            INVARIANT(result, "expected successful query compilation or exception, but got nothing");
            queryIds.push_back(plan.getQueryId());
            compiledPlans.push_back(std::move(result));
        }
        auto preparedPlans = nodeEngine->prepareQueries(std::move(compiledPlans));
        /// Preparation takes channel endpoints. Validate only after every plan has been
        /// prepared; on failure, RAII destroys the prepared plans without starting them.
        for (const auto& channel : sharedObjects->getObjects<HandoffChannel>())
        {
            channel->validate();
        }
        nodeEngine->startQueries(std::move(preparedPlans), successfullyStarted);
        return queryIds;
    }
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_CATCH is declared by public cpptrace/from_current.hpp.
    CPPTRACE_CATCH(...)
    {
        auto initiatingError = wrapExternalException();
        /// The stop wrapper is noexcept and reports cleanup failure through its expected result.
        for (const auto& queryId : successfullyStarted)
        {
            auto stopResult = stopQuery(queryId);
            if (!stopResult)
            {
                NES_WARNING("Could not stop query {} during deployment rollback: {}", queryId, stopResult.error().what());
            }
        }
        return std::unexpected(std::move(initiatingError));
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId) noexcept
{
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_TRY is declared by public cpptrace/from_current.hpp.
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId);
        return {};
    }
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_CATCH is declared by public cpptrace/from_current.hpp.
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<LocalQueryStatusSnapshot, Exception> SingleNodeWorker::getQueryStatus(QueryId queryId) const noexcept
{
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_TRY is declared by public cpptrace/from_current.hpp.
    CPPTRACE_TRY
    {
        auto status = nodeEngine->getQueryLog()->getQueryStatus(queryId);
        if (not status.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return status.value();
    }
    /// NOLINTNEXTLINE(misc-include-cleaner) CPPTRACE_CATCH is declared by public cpptrace/from_current.hpp.
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
                /// A query stopped during setup may never have reported Running.
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
