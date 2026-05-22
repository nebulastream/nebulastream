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
#include <ittnotify.h>
#include <unistd.h>
#include <Configurations/ConfigValuePrinter.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <nes-io-runtime-bindings/lib.h>
#include <nlohmann/json.hpp>
#include <rust/cxx.h>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <IORuntime.hpp>
#include <QueryCompiler.hpp>
#include <QueryStatus.hpp>
#include <QueryTerminationType.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <WorkerStatus.hpp>

namespace
{
__itt_domain* workerDomain = __itt_domain_create("worker");
__itt_string_handle* ittRegisterQuery = __itt_string_handle_create("Register");
__itt_string_handle* ittStartQuery = __itt_string_handle_create("Start");
__itt_string_handle* ittStopQuery = __itt_string_handle_create("Stop");
}

namespace
{
/// Config-registry names. Must match the constants in
/// nes-network/network/src/registry.rs (`SENDER_SERVICE_CONFIG` /
/// `RECEIVER_SERVICE_CONFIG`).
constexpr auto NES_NETWORK_SENDER_CONFIG = "nes-network-sender";
constexpr auto NES_NETWORK_RECEIVER_CONFIG = "nes-network-receiver";
}

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, const Host& host)
    : ioRuntime(std::make_unique<IORuntime>()), listener(std::make_shared<CompositeStatisticListener>()), configuration(configuration)
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

    if (!configuration.dataAddress.getValue().empty())
    {
        const auto& networkConfig = configuration.workerConfiguration.network;
        const auto& connectionAddr = configuration.dataAddress.getValue();
        const nlohmann::json senderConfig{
            {"this_connection", connectionAddr},
            {"sender_queue_size", networkConfig.senderQueueSize.getValue()},
            {"max_pending_acks", networkConfig.maxPendingAcks.getValue()},
        };
        const nlohmann::json receiverConfig{
            {"this_connection", connectionAddr},
            {"receiver_queue_size", networkConfig.receiverQueueSize.getValue()},
        };
        ioRuntime->attachConfig(NES_NETWORK_RECEIVER_CONFIG, receiverConfig);
        ioRuntime->attachConfig(NES_NETWORK_SENDER_CONFIG, senderConfig);
    }

    NES_INFO("IORuntime constructed for worker {}", host.getRawValue());
    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(host);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(configuration.workerConfiguration.defaultQueryExecution);
}

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    __itt_task_begin(workerDomain, __itt_null, __itt_null, ittRegisterQuery);
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

        const LogContext queryIdContext("queryId", plan.getQueryId());
        const LogContext actionContext("action", "register");

        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(plan);
        request->dumpCompilationResult = dumpMode;
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        nodeEngine->registerCompiledQueryPlan(plan.getQueryId(), std::move(result));
        __itt_task_end(workerDomain);
        return plan.getQueryId();
    }
    CPPTRACE_CATCH(...)
    {
        __itt_task_end(workerDomain);
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::startQuery(QueryId queryId) noexcept
{
    __itt_task_begin(workerDomain, __itt_null, __itt_null, ittStartQuery);
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        LogContext queryIdContext("queryId", queryId);
        LogContext actionContext("action", "startQuery");
        nodeEngine->startQuery(queryId);
        __itt_task_end(workerDomain);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        __itt_task_end(workerDomain);
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type) noexcept
{
    __itt_task_begin(workerDomain, __itt_null, __itt_null, ittStopQuery);
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        LogContext queryIdContext("queryId", queryId);
        LogContext actionContext("action", "stopQuery");
        nodeEngine->stopQuery(queryId, type);
        __itt_task_end(workerDomain);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        __itt_task_end(workerDomain);
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
