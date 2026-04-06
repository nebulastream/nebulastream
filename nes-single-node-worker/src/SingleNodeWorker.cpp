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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <typeindex>
#include <utility>
#include <unistd.h>
#include <Configurations/ConfigValuePrinter.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <QueryCompiler.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, const Host& host)
    : workerNodeId(host), listener(std::make_shared<CompositeStatisticListener>()), configuration(configuration)
{
    /// Set up worker-scoped thread-locals. WorkerNodeId is propagated via the
    /// thread init hook map. EventStore uses WorkerLocalSingleton CRTP which
    /// handles hook registration automatically on construction.
    Thread::WorkerNodeId = workerNodeId;
    detail::threadInitHooks[std::type_index(typeid(Host))] = [this]() { Thread::WorkerNodeId = workerNodeId; };
    eventStore = std::make_unique<EventStore>();
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
        networkRuntime = std::make_unique<NetworkRuntime>(
            configuration.dataAddress.getValue(),
            host.getRawValue(),
            NetworkOptions{
                .senderQueueSize = static_cast<uint32_t>(networkConfig.senderQueueSize.getValue()),
                .maxPendingAcks = static_cast<uint32_t>(networkConfig.maxPendingAcks.getValue()),
                .receiverQueueSize = static_cast<uint32_t>(networkConfig.receiverQueueSize.getValue()),
                .senderIOThreads = static_cast<uint32_t>(networkConfig.senderIOThreads.getValue()),
                .receiverIOThreads = static_cast<uint32_t>(networkConfig.receiverIOThreads.getValue()),
            });
    }

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(host);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(configuration.workerConfiguration.defaultQueryExecution);
    registerSystemQueries(host);
}

void SingleNodeWorker::registerSystemQueries(const Host& host)
{
    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    const SemanticAnalyzer analyzer{sourceCatalog, sinkCatalog};
    const RuleBasedOptimizer optimizer{{}};
    const auto hostStr = host.getRawValue();
    /// Sanitize host for use in socket/file paths (replace : with _).
    auto sanitizedHostStr = hostStr;
    std::replace(sanitizedHostStr.begin(), sanitizedHostStr.end(), ':', '_');

    auto startSystemQuery = [&](const std::string& name, std::string sql)
    {
        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
        plan = analyzer.analyse(plan);
        plan = optimizer.optimize(std::move(plan));

        auto queryIdResult = registerQuery(plan);
        if (!queryIdResult.has_value())
        {
            NES_WARNING("Failed to register system query '{}': {}", name, queryIdResult.error());
            return;
        }
        auto startResult = startQuery(queryIdResult.value());
        if (startResult.has_value())
        {
            NES_INFO("System query '{}' started (queryId={})", name, queryIdResult.value());
        }
        else
        {
            NES_WARNING("Failed to start system query '{}': {}", name, startResult.error());
        }
    };

    /// pipeline_compiled → raw events to unix socket
    startSystemQuery(
        "pipeline_compiled",
        fmt::format(
            R"(
        SELECT timestamp_ms, ToUuid(query_id_hi, query_id_lo) AS query_id, pipeline_id, compile_time_ns
        FROM Event(
            'pipeline_compiled' AS `SOURCE`.EVENT_NAME,
            '{0}' AS `SOURCE`.`HOST`,
            '1000' AS `SOURCE`.FLUSH_INTERVAL_MS,
            'true' AS `SOURCE`.INCLUDE_TIMESTAMP,
            SCHEMA(timestamp_ms UINT64 NOT NULL, query_id_hi UINT64 NOT NULL, query_id_lo UINT64 NOT NULL, pipeline_id UINT64 NOT NULL, compile_time_ns UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`,
            'native' AS PARSER.`TYPE`)
        INTO GrpcStream('pipeline_compiled' AS `SINK`.STAT_TYPE, '{0}' AS `SINK`.`HOST`, 'CSV' AS `SINK`.OUTPUT_FORMAT)
    )",
            hostStr,
            sanitizedHostStr));

    /// query_status → raw events to unix socket
    startSystemQuery(
        "query_status",
        fmt::format(
            R"(
        SELECT timestamp_ms, ToUuid(query_id_hi, query_id_lo) AS query_id, status
        FROM Event(
            'query_status' AS `SOURCE`.EVENT_NAME,
            '{0}' AS `SOURCE`.`HOST`,
            '1000' AS `SOURCE`.FLUSH_INTERVAL_MS,
            'true' AS `SOURCE`.INCLUDE_TIMESTAMP,
            SCHEMA(timestamp_ms UINT64 NOT NULL, query_id_hi UINT64 NOT NULL, query_id_lo UINT64 NOT NULL, status VARSIZED NOT NULL) AS `SOURCE`.`SCHEMA`,
            'native' AS PARSER.`TYPE`)
        INTO GrpcStream('query_status' AS `SINK`.STAT_TYPE, '{0}' AS `SINK`.`HOST`, 'CSV' AS `SINK`.OUTPUT_FORMAT)
    )",
            hostStr,
            sanitizedHostStr));

    /// unpooled_buffer_alloc → COUNT and AVG over 1-second tumbling window
    startSystemQuery(
        "unpooled_buffer_alloc",
        fmt::format(
            R"(
        SELECT start, end, COUNT(alloc_size) AS alloc_count, AVG(alloc_size) AS avg_alloc_size
        FROM Event(
            'unpooled_buffer_alloc' AS `SOURCE`.EVENT_NAME,
            '{0}' AS `SOURCE`.`HOST`,
            '500' AS `SOURCE`.FLUSH_INTERVAL_MS,
            'true' AS `SOURCE`.INCLUDE_TIMESTAMP,
            SCHEMA(timestamp_ms UINT64 NOT NULL, alloc_size UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`,
            'native' AS PARSER.`TYPE`)
        WINDOW TUMBLING(timestamp_ms, SIZE 1 SEC)
        INTO GrpcStream('unpooled_buffer_alloc' AS `SINK`.STAT_TYPE, '{0}' AS `SINK`.`HOST`, 'CSV' AS `SINK`.OUTPUT_FORMAT)
    )",
            hostStr,
            sanitizedHostStr));

    /// buffer_ingestion → per-query throughput over 1-second tumbling window, keyed by query ID
    startSystemQuery(
        "buffer_ingestion",
        fmt::format(
            R"(
        SELECT ToUuid(query_id_hi, query_id_lo) AS query_id, start, end, SUM(num_tuples) AS total_tuples, COUNT(num_tuples) AS ingestion_count
        FROM Event(
            'buffer_ingestion' AS `SOURCE`.EVENT_NAME,
            '{0}' AS `SOURCE`.`HOST`,
            '500' AS `SOURCE`.FLUSH_INTERVAL_MS,
            'true' AS `SOURCE`.INCLUDE_TIMESTAMP,
            SCHEMA(timestamp_ms UINT64 NOT NULL, query_id_hi UINT64 NOT NULL, query_id_lo UINT64 NOT NULL, origin_id UINT64 NOT NULL, num_tuples UINT64 NOT NULL) AS `SOURCE`.`SCHEMA`,
            'native' AS PARSER.`TYPE`)
        WINDOW TUMBLING(timestamp_ms, SIZE 1 SEC)
        GROUP BY query_id_hi, query_id_lo
        INTO GrpcStream('buffer_ingestion' AS `SINK`.STAT_TYPE, '{0}' AS `SINK`.`HOST`, 'CSV' AS `SINK`.OUTPUT_FORMAT)
    )",
            hostStr,
            sanitizedHostStr));
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

        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(plan);
        request->dumpCompilationResult = dumpMode;
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        nodeEngine->registerCompiledQueryPlan(plan.getQueryId(), std::move(result));
        return plan.getQueryId();
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
