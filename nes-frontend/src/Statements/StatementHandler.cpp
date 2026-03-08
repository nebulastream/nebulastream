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

#include <Statements/StatementHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Replay/TimeTravelReadResolver.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Pointers.hpp>
#include <Util/Ranges.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{
std::string formatOptionalMilliseconds(const std::optional<uint64_t>& durationMs)
{
    return durationMs.transform([](const auto value) { return fmt::format("{} ms", value); }).value_or("none");
}

std::string recordingDecisionToString(RecordingSelectionDecision decision)
{
    switch (decision)
    {
        case RecordingSelectionDecision::CreateNewRecording:
            return "create_new_recording";
        case RecordingSelectionDecision::UpgradeExistingRecording:
            return "upgrade_existing_recording";
        case RecordingSelectionDecision::ReuseExistingRecording:
            return "reuse_existing_recording";
    }
    std::unreachable();
}

std::string recordingRepresentationToString(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return "binary_store";
        case RecordingRepresentation::BinaryStoreZstd:
            return "binary_store_zstd";
    }
    std::unreachable();
}

void appendCostBreakdown(
    std::stringstream& explainMessage,
    std::string_view prefix,
    const RecordingCostBreakdown& costBreakdown,
    std::string_view decisionLabel)
{
    fmt::println(
        explainMessage,
        "{} decision={} total_cost={:.2f} boundary_cut_cost={:.2f} replay_recompute_cost={:.2f} maintenance_cost={:.2f}"
        " replay_cost={:.2f} recompute_cost={:.2f} replay_time_multiplier={:.2f} storage_bytes={} replay_bandwidth={}B/s"
        " replay_latency={} ms operators={} fits_budget={} satisfies_latency={}",
        prefix,
        decisionLabel,
        costBreakdown.totalCost(),
        costBreakdown.boundaryCutCost,
        costBreakdown.replayRecomputeCost,
        costBreakdown.maintenanceCost,
        costBreakdown.replayCost,
        costBreakdown.recomputeCost,
        costBreakdown.replayTimeMultiplier,
        costBreakdown.estimatedStorageBytes,
        costBreakdown.estimatedReplayBandwidthBytesPerSecond,
        costBreakdown.estimatedReplayLatencyMs,
        costBreakdown.operatorCount,
        costBreakdown.fitsBudget,
        costBreakdown.satisfiesReplayLatency);
}

void appendReplayExplainSection(std::stringstream& explainMessage, const DistributedLogicalPlan& distributedPlan)
{
    const auto& replaySpecification = distributedPlan.getReplaySpecification();
    const auto& selectionResult = distributedPlan.getRecordingSelectionResult();
    const auto& selectedRecordings = selectionResult.selectedRecordings;
    if (!replaySpecification.has_value() && selectedRecordings.empty())
    {
        return;
    }

    fmt::println(explainMessage, "Replay Recording Selection:");
    fmt::println(
        explainMessage,
        "Requested retention window: {}",
        formatOptionalMilliseconds(replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; })));
    fmt::println(
        explainMessage,
        "Requested replay latency limit: {}",
        formatOptionalMilliseconds(replaySpecification.and_then([](const auto& spec) { return spec.replayLatencyLimitMs; })));

    if (selectedRecordings.empty())
    {
        fmt::println(explainMessage, "Selected recording boundary: none");
        return;
    }

    fmt::println(explainMessage, "Selected recording boundary:");
    for (const auto& selection : selectedRecordings)
    {
        fmt::println(
            explainMessage,
            "- recording_id={} node={} representation={} file={}",
            selection.recordingId.getRawValue(),
            selection.node,
            recordingRepresentationToString(selection.representation),
            selection.filePath);
    }

    if (selectionResult.explanations.empty())
    {
        return;
    }

    fmt::println(explainMessage, "Replay decision reasoning:");
    for (const auto& explanation : selectionResult.explanations)
    {
        fmt::println(
            explainMessage,
            "- recording_id={} node={} representation={} decision={} reason={}",
            explanation.selection.recordingId.getRawValue(),
            explanation.selection.node,
            recordingRepresentationToString(explanation.selection.representation),
            recordingDecisionToString(explanation.decision),
            explanation.reason);
        appendCostBreakdown(
            explainMessage,
            "  chosen_cost:",
            explanation.chosenCost,
            recordingDecisionToString(explanation.decision));
        for (const auto& alternative : explanation.alternatives)
        {
            appendCostBreakdown(
                explainMessage,
                "  alternative_cost:",
                alternative.cost,
                recordingDecisionToString(alternative.decision));
        }
    }
}

struct ActiveQueryRedeployment
{
    DistributedQueryId queryId;
    LogicalPlan originalPlan;
    DistributedLogicalPlan redeployedPlan;
    bool shouldRestart;
};

std::vector<DistributedQueryId>
collectActiveReplayRedeployQueries(const RecordingSelectionResult& selectionResult, const DistributedQueryId& incomingQueryId)
{
    std::vector<DistributedQueryId> queryIds;
    for (const auto& explanation : selectionResult.networkExplanations)
    {
        if (explanation.selection.coversIncomingQuery)
        {
            continue;
        }
        if (explanation.decision != RecordingSelectionDecision::CreateNewRecording
            && explanation.decision != RecordingSelectionDecision::UpgradeExistingRecording)
        {
            continue;
        }

        for (const auto& beneficiaryQuery : explanation.selection.beneficiaryQueries)
        {
            const auto queryId = DistributedQueryId(beneficiaryQuery);
            if (queryId == incomingQueryId || std::ranges::contains(queryIds, queryId))
            {
                continue;
            }
            queryIds.push_back(queryId);
        }
    }
    return queryIds;
}

std::expected<void, Exception> stopQueryIfRunning(SharedPtr<QueryManager>& queryManager, const DistributedQueryId& queryId)
{
    const auto statusResult = queryManager->status(queryId);
    if (!statusResult)
    {
        return std::unexpected(QueryStatusFailed(
            "Could not inspect query {} before replay redeployment: {}",
            queryId,
            fmt::join(statusResult.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", ")));
    }

    const auto globalState = statusResult->getGlobalQueryState();
    if (globalState != DistributedQueryState::Running && globalState != DistributedQueryState::PartiallyStopped)
    {
        return {};
    }

    return queryManager->stop(queryId)
        .transform_error(
            [&queryId](const auto& errors)
            {
                return QueryStopFailed(
                    "Could not stop query {} for replay redeployment: {}",
                    queryId,
                    fmt::join(errors | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
            });
}

std::expected<void, Exception> unregisterQueryForRedeployment(SharedPtr<QueryManager>& queryManager, const DistributedQueryId& queryId)
{
    return queryManager->unregister(queryId)
        .transform_error(
            [&queryId](const auto& errors)
            {
                return QueryUnregistrationFailed(
                    "Could not unregister query {} for replay redeployment: {}",
                    queryId,
                    fmt::join(errors | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
            });
}

std::expected<void, Exception> startRedeployedQueryIfNeeded(
    SharedPtr<QueryManager>& queryManager, const DistributedQueryId& queryId, const bool shouldRestart)
{
    if (!shouldRestart)
    {
        return {};
    }

    return queryManager->start(queryId)
        .transform_error(
            [&queryId](const auto& errors)
            {
                return QueryStartFailed(
                    "Could not restart query {} after replay redeployment: {}",
                    queryId,
                    fmt::join(errors | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
            });
}

std::expected<std::vector<ActiveQueryRedeployment>, Exception> buildActiveReplayRedeployments(
    SharedPtr<QueryManager>& queryManager,
    const SharedPtr<const LegacyOptimizer>& optimizer,
    const SharedPtr<const SourceCatalog>& sourceCatalog,
    const RecordingSelectionResult& selectionResult,
    const DistributedQueryId& incomingQueryId)
{
    std::vector<ActiveQueryRedeployment> redeployments;
    for (const auto& queryId : collectActiveReplayRedeployQueries(selectionResult, incomingQueryId))
    {
        const auto metadataIt = queryManager->getRecordingCatalog().getQueryMetadata().find(queryId);
        if (metadataIt == queryManager->getRecordingCatalog().getQueryMetadata().end())
        {
            continue;
        }

        const auto& metadata = metadataIt->second;
        if (!metadata.originalPlan.has_value() || !metadata.replaySpecification.has_value())
        {
            continue;
        }

        auto statusResult = queryManager->status(queryId);
        if (!statusResult)
        {
            return std::unexpected(QueryStatusFailed(
                "Could not inspect query {} before replay redeployment planning: {}",
                queryId,
                fmt::join(statusResult.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", ")));
        }

        auto originalPlan = *metadata.originalPlan;
        resolveTimeTravelReadSources(originalPlan, sourceCatalog);
        auto redeployedPlan = optimizer->optimize(originalPlan, metadata.replaySpecification, queryManager->getRecordingCatalog());
        redeployedPlan.setQueryId(queryId);

        redeployments.push_back(ActiveQueryRedeployment{
            .queryId = queryId,
            .originalPlan = *metadata.originalPlan,
            .redeployedPlan = std::move(redeployedPlan),
            .shouldRestart = statusResult->getGlobalQueryState() == DistributedQueryState::Running
                || statusResult->getGlobalQueryState() == DistributedQueryState::PartiallyStopped});
    }
    return redeployments;
}

std::expected<void, Exception> applyActiveReplayRedeployments(
    SharedPtr<QueryManager>& queryManager, std::vector<ActiveQueryRedeployment> redeployments)
{
    for (auto& redeployment : redeployments)
    {
        if (auto stopped = stopQueryIfRunning(queryManager, redeployment.queryId); !stopped)
        {
            return std::unexpected(stopped.error());
        }

        if (auto unregistered = unregisterQueryForRedeployment(queryManager, redeployment.queryId); !unregistered)
        {
            return std::unexpected(unregistered.error());
        }

        const auto registerResult = queryManager->registerQuery(redeployment.redeployedPlan, redeployment.originalPlan);
        if (!registerResult)
        {
            return std::unexpected(QueryRegistrationFailed(
                "Could not register query {} after replay redeployment: {}",
                redeployment.queryId,
                registerResult.error().what()));
        }

        if (*registerResult != redeployment.queryId)
        {
            return std::unexpected(QueryRegistrationFailed(
                "Replay redeployment registered query {} under unexpected id {}",
                redeployment.queryId,
                *registerResult));
        }

        if (auto restarted = startRedeployedQueryIfNeeded(queryManager, redeployment.queryId, redeployment.shouldRestart); !restarted)
        {
            return std::unexpected(restarted.error());
        }
    }
    return {};
}
}

SourceStatementHandler::SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog, HostPolicy hostPolicy)
    : sourceCatalog(sourceCatalog), hostPolicy(std::move(hostPolicy))
{
}

std::expected<CreateLogicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreateLogicalSourceStatement& statement)
{
    if (const auto created = sourceCatalog->addLogicalSource(statement.name, statement.schema))
    {
        return CreateLogicalSourceStatementResult{created.value()};
    }
    return std::unexpected{SourceAlreadyExists(statement.name)};
}

std::expected<CreatePhysicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreatePhysicalSourceStatement& statement)
{
    auto logicalSource = sourceCatalog->getLogicalSource(statement.attachedTo.getRawValue());
    if (!logicalSource)
    {
        return std::unexpected{UnknownSourceName(statement.attachedTo.getRawValue())};
    }

    auto sourceConfig = statement.sourceConfig;
    const auto host = [&]
    {
        if (auto it = sourceConfig.find("host"); it != sourceConfig.end())
        {
            const auto host = it->second;
            sourceConfig.erase(it);
            return host;
        }
        return std::visit(
            Overloaded{
                [](const DefaultHost& defaultHost) -> std::string { return defaultHost.hostName; },
                [](const RequireHostConfig&) -> std::string
                { throw InvalidStatement("Could not handle source statement. `SOURCE`.`HOST` was not set"); }},
            hostPolicy);
    }();

    if (const auto created
        = sourceCatalog->addPhysicalSource(*logicalSource, statement.sourceType, Host(host), sourceConfig, statement.parserConfig))
    {
        return CreatePhysicalSourceStatementResult{created.value()};
    }
    return std::unexpected{InvalidConfigParameter("Invalid configuration: {}", statement)};
}

std::expected<ShowLogicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowLogicalSourcesStatement& statement) const
{
    if (statement.name)
    {
        if (const auto foundSource = sourceCatalog->getLogicalSource(*statement.name))
        {
            return ShowLogicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowLogicalSourcesStatementResult{{}};
    }
    return ShowLogicalSourcesStatementResult{sourceCatalog->getAllLogicalSources() | std::ranges::to<std::vector>()};
}

std::expected<ShowPhysicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowPhysicalSourcesStatement& statement) const
{
    if (statement.id and not statement.logicalSource)
    {
        if (const auto foundSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{statement.id.value()}))
        {
            return ShowPhysicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (not statement.id and statement.logicalSource)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(statement.logicalSource->getRawValue()))
        {
            if (const auto foundSources = sourceCatalog->getPhysicalSources(*logicalSource))
            {
                return ShowPhysicalSourcesStatementResult{*foundSources | std::ranges::to<std::vector>()};
            }
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (statement.logicalSource and statement.id)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(statement.logicalSource->getRawValue()))
        {
            if (const auto foundSources = sourceCatalog->getPhysicalSources(*logicalSource))
            {
                return ShowPhysicalSourcesStatementResult{
                    foundSources.value()
                    | std::views::filter([statement](const auto& source)
                                         { return source.getPhysicalSourceId() == PhysicalSourceId{statement.id.value()}; })
                    | std::ranges::to<std::vector>()};
            }
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    return ShowPhysicalSourcesStatementResult{
        sourceCatalog->getLogicalToPhysicalSourceMapping() | std::views::transform([](auto& pair) { return pair.second; })
        | std::views::join | std::ranges::to<std::vector>()};
}

std::expected<DropLogicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropLogicalSourceStatement& statement)
{
    if (auto logical = sourceCatalog->getLogicalSource(statement.source.getRawValue()))
    {
        if (sourceCatalog->removeLogicalSource(*logical))
        {
            return DropLogicalSourceStatementResult{.dropped = statement.source, .schema = *logical->getSchema()};
        }
    }
    return std::unexpected{UnknownSourceName(statement.source.getRawValue())};
}

std::expected<DropPhysicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropPhysicalSourceStatement& statement)
{
    if (sourceCatalog->removePhysicalSource(statement.descriptor))
    {
        return DropPhysicalSourceStatementResult{statement.descriptor};
    }
    return std::unexpected{UnknownSourceName("Unknown physical source: {}", statement.descriptor)};
}

SinkStatementHandler::SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog, HostPolicy hostPolicy)
    : sinkCatalog(sinkCatalog), hostPolicy(std::move(hostPolicy))
{
}

std::expected<CreateSinkStatementResult, Exception> SinkStatementHandler::operator()(const CreateSinkStatement& statement)
{
    auto sinkConfig = statement.sinkConfig;
    const auto host = [&]
    {
        if (auto it = sinkConfig.find("host"); it != sinkConfig.end())
        {
            const auto host = it->second;
            sinkConfig.erase(it);
            return host;
        }
        return std::visit(
            Overloaded{
                [](const DefaultHost& defaultHost) -> std::string { return defaultHost.hostName; },
                [](const RequireHostConfig&) -> std::string
                { throw InvalidStatement("Could not handle sink statement. `SINK`.`HOST` was not set"); }},
            hostPolicy);
    }();

    if (const auto created = sinkCatalog->addSinkDescriptor(statement.name, statement.schema, statement.sinkType, Host(host), sinkConfig))
    {
        return CreateSinkStatementResult{created.value()};
    }
    return std::unexpected{SinkAlreadyExists(statement.name)};
}

std::expected<ShowSinksStatementResult, Exception> SinkStatementHandler::operator()(const ShowSinksStatement& statement) const
{
    if (statement.name)
    {
        if (const auto foundSink = sinkCatalog->getSinkDescriptor(*statement.name))
        {
            return ShowSinksStatementResult{std::vector{*foundSink}};
        }
        return ShowSinksStatementResult{{}};
    }
    return ShowSinksStatementResult{sinkCatalog->getAllSinkDescriptors()};
}

std::expected<DropSinkStatementResult, Exception> SinkStatementHandler::operator()(const DropSinkStatement& statement)
{
    const auto sink = sinkCatalog->getSinkDescriptor(statement.name);
    if (not sink.has_value())
    {
        throw UnknownSinkName("Cannot remove unknown sink: {}", statement.name);
    }
    if (sinkCatalog->removeSinkDescriptor(sink.value()))
    {
        return DropSinkStatementResult{sink.value()};
    }
    return std::unexpected{UnknownSinkName(statement.name)};
}

QueryStatementHandler::QueryStatementHandler(
    SharedPtr<QueryManager> queryManager, SharedPtr<const LegacyOptimizer> optimizer, SharedPtr<const SourceCatalog> sourceCatalog)
    : queryManager(std::move(queryManager)), optimizer(std::move(optimizer)), sourceCatalog(std::move(sourceCatalog))
{
}

std::expected<DropQueryStatementResult, Exception> QueryStatementHandler::operator()(const DropQueryStatement& statement)
{
    auto stopResult = queryManager->stop(statement.id)
                          .and_then([&statement, this] { return queryManager->unregister(statement.id); })
                          .transform_error(
                              [](auto vecOfErrors)
                              {
                                  return QueryStopFailed(
                                      "Could not stop query: {}",
                                      fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
                              })
                          .transform([&statement] { return DropQueryStatementResult{statement.id}; });

    return stopResult;
}

std::expected<ExplainQueryStatementResult, Exception> QueryStatementHandler::operator()(const ExplainQueryStatement& statement)
{
    CPPTRACE_TRY
    {
        auto plan = statement.plan;
        resolveTimeTravelReadSources(plan, sourceCatalog);
        queryManager->refreshWorkerMetrics();
        std::stringstream explainMessage;
        fmt::println(explainMessage, "Query:\n{}", plan.getOriginalSql());
        fmt::println(explainMessage, "Initial Logical Plan:\n{}", plan);

        const auto distributedPlan = optimizer->optimize(plan, statement.replaySpecification, queryManager->getRecordingCatalog());

        appendReplayExplainSection(explainMessage, distributedPlan);
        fmt::println(explainMessage, "Optimized Global Plan:\n{}", distributedPlan.getGlobalPlan());

        fmt::println(explainMessage, "Decomposed Plans:");
        for (const auto& [worker, plans] : distributedPlan)
        {
            fmt::println(explainMessage, "{} plans on {}:", plans.size(), worker);
            for (const auto& [index, plan] : plans | views::enumerate)
            {
                fmt::println(explainMessage, "{}:\n{}\n", index, plan);
            }
        }
        return ExplainQueryStatementResult{explainMessage.str()};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<QueryStatementResult, Exception> QueryStatementHandler::operator()(const QueryStatement& statement)
{
    CPPTRACE_TRY
    {
        auto plan = statement.plan;
        resolveTimeTravelReadSources(plan, sourceCatalog);
        queryManager->refreshWorkerMetrics();
        auto distributedPlan = optimizer->optimize(plan, statement.replaySpecification, queryManager->getRecordingCatalog());

        if (statement.id)
        {
            distributedPlan.setQueryId(*statement.id);
        }

        const auto queryResult = queryManager->registerQuery(distributedPlan, statement.plan);
        if (!queryResult)
        {
            return std::unexpected(queryResult.error());
        }

        const auto queryId = *queryResult;
        const auto redeployments = buildActiveReplayRedeployments(
            queryManager, optimizer, sourceCatalog, distributedPlan.getRecordingSelectionResult(), queryId);
        if (!redeployments)
        {
            (void)queryManager->unregister(queryId);
            return std::unexpected(redeployments.error());
        }

        if (const auto redeploymentResult = applyActiveReplayRedeployments(queryManager, *redeployments); !redeploymentResult)
        {
            (void)queryManager->unregister(queryId);
            return std::unexpected(redeploymentResult.error());
        }

        const auto startResult = queryManager->start(queryId).transform_error(
            [](auto vecOfErrors)
            {
                return QueryStartFailed(
                    "Could not start query: {}",
                    fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
            });
        if (!startResult)
        {
            (void)queryManager->unregister(queryId);
            return std::unexpected(startResult.error());
        }
        return QueryStatementResult{queryId};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

TopologyStatementHandler::TopologyStatementHandler(SharedPtr<QueryManager> queryManager, SharedPtr<WorkerCatalog> workerCatalog)
    : queryManager(std::move(queryManager)), workerCatalog(std::move(workerCatalog))
{
}

std::expected<WorkerStatusStatementResult, Exception> TopologyStatementHandler::operator()(const WorkerStatusStatement& statement)
{
    auto statusResult = queryManager->workerStatus(std::chrono::system_clock::time_point(std::chrono::milliseconds(0)));
    if (!statusResult)
    {
        return std::unexpected(statusResult.error());
    }
    auto status = statusResult.value();

    if (statement.host.empty())
    {
        return WorkerStatusStatementResult{status};
    }

    std::erase_if(
        status.workerStatus,
        [&](const auto& it)
        {
            auto found = std::ranges::find(statement.host, it.first.getRawValue());
            return found != statement.host.end();
        });

    return WorkerStatusStatementResult{status};
}

std::expected<CreateWorkerStatementResult, Exception> TopologyStatementHandler::operator()(const CreateWorkerStatement& statement)
{
    SingleNodeWorkerConfiguration config;
    if (!statement.config.empty())
    {
        config.overwriteConfigWithCommandLineInput(statement.config);
    }
    workerCatalog->addWorker(
        Host(statement.host),
        statement.data,
        statement.capacity.value_or(INFINITE_CAPACITY),
        statement.downstream | std::views::transform([](auto downstream) { return Host(std::move(downstream)); })
            | std::ranges::to<std::vector>(),
        std::move(config),
        statement.recordingStorageBudget.value_or(INFINITE_CAPACITY));
    return CreateWorkerStatementResult{Host(statement.host)};
}

std::expected<DropWorkerStatementResult, Exception> TopologyStatementHandler::operator()(const DropWorkerStatement& statement)
{
    const auto workerConfigOpt = workerCatalog->removeWorker(Host(statement.host));
    if (workerConfigOpt)
    {
        return DropWorkerStatementResult{workerConfigOpt->host};
    }
    return std::unexpected(UnknownWorker(": '{}'", statement.host));
}

std::expected<SetRecordingStorageStatementResult, Exception> TopologyStatementHandler::operator()(const SetRecordingStorageStatement& statement)
{
    const auto host = Host(statement.host);
    if (!workerCatalog->setRecordingStorageBudget(host, statement.recordingStorageBudget))
    {
        return std::unexpected(UnknownWorker(": '{}'", statement.host));
    }
    return SetRecordingStorageStatementResult{.host = host, .recordingStorageBudget = statement.recordingStorageBudget};
}

std::expected<ShowRecordingStorageStatementResult, Exception>
TopologyStatementHandler::operator()(const ShowRecordingStorageStatement& statement) const
{
    if (statement.host.has_value())
    {
        if (const auto worker = workerCatalog->getWorker(Host(*statement.host)); worker.has_value())
        {
            return ShowRecordingStorageStatementResult{
                .workers = {{.host = worker->host, .recordingStorageBudget = worker->recordingStorageBudget}}};
        }
        return ShowRecordingStorageStatementResult{.workers = {}};
    }

    auto workers = workerCatalog->getAllWorkers()
        | std::views::transform(
              [](const WorkerConfig& worker)
              { return RecordingStorageEntry{.host = worker.host, .recordingStorageBudget = worker.recordingStorageBudget}; })
        | std::ranges::to<std::vector>();
    std::ranges::sort(workers, {}, [](const RecordingStorageEntry& worker) { return worker.host.getRawValue(); });
    return ShowRecordingStorageStatementResult{.workers = std::move(workers)};
}

std::expected<ShowQueriesStatementResult, Exception> QueryStatementHandler::operator()(const ShowQueriesStatement& statement)
{
    if (not statement.id.has_value())
    {
        auto statusResults
            = queryManager->queries()
            | std::views::transform(
                  [&](const auto& queryId) -> std::pair<DistributedQueryId, std::expected<DistributedQueryStatus, Exception>>
                  {
                      auto statusResult = queryManager->status(queryId).transform_error(
                          [](auto vecOfErrors)
                          {
                              return QueryStatusFailed(
                                  "Could not fetch status for query: {}",
                                  fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
                          });
                      return {queryId, statusResult};
                  })
            | std::ranges::to<std::vector>();

        auto failedStatusResults = statusResults
            | std::views::filter([](const auto& idAndStatusResult) { return !idAndStatusResult.second.has_value(); })
            | std::views::transform([](const auto& idAndStatusResult) -> std::pair<DistributedQueryId, Exception>
                                    { return {idAndStatusResult.first, idAndStatusResult.second.error()}; });

        auto goodQueryStatusResults = statusResults
            | std::views::filter([](const auto& idAndStatusResult) { return idAndStatusResult.second.has_value(); })
            | std::views::transform([](const auto& idAndStatusResult) -> std::pair<DistributedQueryId, DistributedQueryStatus>
                                    { return {idAndStatusResult.first, idAndStatusResult.second.value()}; });
        if (!failedStatusResults.empty())
        {
            return std::unexpected(
                QueryStatusFailed("Could not retrieve query status for some queries: ", fmt::join(failedStatusResults, "\n")));
        }

        return ShowQueriesStatementResult{
            goodQueryStatusResults | std::ranges::to<std::unordered_map<DistributedQueryId, DistributedQueryStatus>>()};
    }

    const auto statusOpt = queryManager->status(statement.id.value());
    if (statusOpt)
    {
        return ShowQueriesStatementResult{
            std::unordered_map<DistributedQueryId, DistributedQueryStatus>{{statement.id.value(), statusOpt.value()}}};
    }
    return std::unexpected(QueryStatusFailed("Could not retrieve query status for some queries: ", fmt::join(statusOpt.error(), "\n")));
}
}
