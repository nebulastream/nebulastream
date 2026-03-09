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
#include <thread>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Replay/ReplayStorage.hpp>
#include <Replay/TimeTravelReadResolver.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
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
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return "binary_store_zstd_fast1";
        case RecordingRepresentation::BinaryStoreZstd:
            return "binary_store_zstd";
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return "binary_store_zstd_fast6";
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
        " replay_cost={:.2f} recompute_cost={:.2f} replay_time_multiplier={:.2f} storage_bytes={} incremental_storage_bytes={} replay_bandwidth={}B/s"
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
        costBreakdown.incrementalStorageBytes,
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

struct RecordingEpochDeployment
{
    DistributedQueryId beneficiaryQueryId;
    DistributedLogicalPlan epochPlan;
};

struct ReplayBoundaryEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingRewriteEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

struct PreparedReplayRecording
{
    RecordingId recordingId{RecordingId::INVALID};
    std::string filePath;
    std::vector<uint64_t> segmentIds;
};

struct ReplaySourceReplacement
{
    Host host{Host::INVALID};
    std::string recordingId;
    std::string filePath;
    std::vector<uint64_t> segmentIds;
    uint64_t intervalStartMs = 0;
    uint64_t intervalEndMs = 0;
};

struct PreparedReplayExecutionPlan
{
    LogicalPlan replayPlan;
    std::vector<PreparedReplayRecording> pinnedRecordings;
};

std::string joinExceptionMessages(const std::vector<Exception>& exceptions)
{
    return fmt::format(
        "{}",
        fmt::join(
            exceptions | std::views::transform([](const auto& exception) { return std::string(exception.what()); }),
            ", "));
}

std::string encodeSegmentIds(const std::vector<uint64_t>& segmentIds)
{
    std::stringstream encoded;
    for (size_t index = 0; index < segmentIds.size(); ++index)
    {
        if (index > 0)
        {
            encoded << ',';
        }
        encoded << segmentIds[index];
    }
    return encoded.str();
}

LogicalOperator buildReplaySourceOperator(
    const SharedPtr<const SourceCatalog>& sourceCatalog, const LogicalOperator& replacedChild, const ReplaySourceReplacement& replacement)
{
    std::unordered_map<std::string, std::string> sourceConfig{
        {"host", replacement.host.getRawValue()},
        {"file_path", replacement.filePath},
        {"recording_id", replacement.recordingId},
        {"segment_ids", encodeSegmentIds(replacement.segmentIds)},
        {"scan_start_ms", std::to_string(replacement.intervalStartMs)},
        {"scan_end_ms", std::to_string(replacement.intervalEndMs)},
        {"replay_mode", "interval"}};
    std::unordered_map<std::string, std::string> parserConfig{{"type", "NATIVE"}};
    const auto descriptor = sourceCatalog->getInlineSource("BinaryStore", replacedChild.getOutputSchema(), std::move(parserConfig), std::move(sourceConfig));
    if (!descriptor.has_value())
    {
        throw InvalidConfigParameter("Could not create replay source descriptor for recording {}", replacement.recordingId);
    }
    return SourceDescriptorLogicalOperator(*descriptor).withTraitSet(replacedChild.getTraitSet());
}

LogicalOperator rewriteReplayBoundariesWithSources(
    const LogicalOperator& current,
    const SharedPtr<const SourceCatalog>& sourceCatalog,
    const std::unordered_map<RecordingRewriteEdge, ReplaySourceReplacement, ReplayBoundaryEdgeHash>& replacements)
{
    const auto originalChildren = current.getChildren();
    std::vector<LogicalOperator> rewrittenChildren;
    rewrittenChildren.reserve(originalChildren.size());
    for (const auto& child : originalChildren)
    {
        const RecordingRewriteEdge edge{.parentId = current.getId(), .childId = child.getId()};
        if (const auto replacementIt = replacements.find(edge); replacementIt != replacements.end())
        {
            rewrittenChildren.push_back(buildReplaySourceOperator(sourceCatalog, child, replacementIt->second));
            continue;
        }
        rewrittenChildren.push_back(rewriteReplayBoundariesWithSources(child, sourceCatalog, replacements));
    }
    return current.withChildren(std::move(rewrittenChildren));
}

LogicalPlan buildReplaySourcePlan(
    const LogicalPlan& basePlan,
    const SharedPtr<const SourceCatalog>& sourceCatalog,
    const std::unordered_map<RecordingRewriteEdge, ReplaySourceReplacement, ReplayBoundaryEdgeHash>& replacements)
{
    auto rewrittenRoots = basePlan.getRootOperators();
    for (auto& root : rewrittenRoots)
    {
        root = rewriteReplayBoundariesWithSources(root, sourceCatalog, replacements);
    }
    return basePlan.withRootOperators(std::move(rewrittenRoots));
}

std::optional<Exception> unsupportedReplayOperator(const LogicalOperator& current)
{
    if (const auto source = current.tryGetAs<SourceDescriptorLogicalOperator>(); source.has_value())
    {
        if (source->get().getSourceDescriptor().getSourceType() == "BinaryStore")
        {
            return std::nullopt;
        }
        return UnsupportedQuery(
            "Replay execution currently requires all replay suffix inputs to come from maintained recordings, but replay plan still"
            " depends on live source operator {}",
            current.getId());
    }
    if (current.getName() == "Source")
    {
        return UnsupportedQuery(
            "Replay execution currently requires all replay suffix inputs to come from maintained recordings, but replay plan still"
            " depends on live source operator {}",
            current.getId());
    }
    if (current.getName() == "Join" || current.getName() == "WindowedAggregation")
    {
        return UnsupportedQuery(
            "Replay execution without checkpoints does not support replay suffix operator {} ({})",
            current.getName(),
            current.getId());
    }
    if (current.getName() == "Store")
    {
        return UnsupportedQuery(
            "Replay execution expected persisted replay suffix plans without Store operators, but found Store operator {}",
            current.getId());
    }
    return std::nullopt;
}

std::expected<void, Exception> validatePhase5ReplayPlan(const LogicalOperator& current)
{
    if (const auto unsupported = unsupportedReplayOperator(current); unsupported.has_value())
    {
        return std::unexpected(*unsupported);
    }
    for (const auto& child : current.getChildren())
    {
        if (auto validation = validatePhase5ReplayPlan(child); !validation)
        {
            return validation;
        }
    }
    return {};
}

std::expected<void, Exception> validatePhase5ReplayPlan(const LogicalPlan& replayPlan)
{
    for (const auto& root : replayPlan.getRootOperators())
    {
        if (auto validation = validatePhase5ReplayPlan(root); !validation)
        {
            return validation;
        }
    }
    return {};
}

void appendPinnedSegment(std::vector<ReplayPinnedSegment>& pinnedSegments, const RecordingId& recordingId, const uint64_t segmentId)
{
    const ReplayPinnedSegment pinnedSegment{.recordingId = recordingId.getRawValue(), .segmentId = segmentId};
    if (!std::ranges::contains(pinnedSegments, pinnedSegment))
    {
        pinnedSegments.push_back(pinnedSegment);
    }
}

std::expected<PreparedReplayExecutionPlan, Exception> buildPreparedReplayExecutionPlan(
    const SharedPtr<const SourceCatalog>& sourceCatalog, const ReplayExecution& execution, const ReplayPlan& replayPlan)
{
    std::unordered_map<std::string, PreparedReplayRecording> pinnedRecordingsById;
    std::unordered_map<RecordingRewriteEdge, ReplaySourceReplacement, ReplayBoundaryEdgeHash> replacements;
    const Replay::BinaryStoreReplaySelection selection{
        .segmentIds = std::nullopt,
        .scanStartMs = execution.intervalStartMs,
        .scanEndMs = execution.intervalEndMs};

    for (const auto& replayBoundary : replayPlan.selectedReplayBoundaries)
    {
        if (replayBoundary.selection.filePath.empty())
        {
            return std::unexpected(UnsupportedQuery(
                "Replay execution requires a concrete recording file for recording {}",
                replayBoundary.selection.recordingId));
        }

        const auto recordingKey = replayBoundary.selection.recordingId.getRawValue();
        auto recordingIt = pinnedRecordingsById.find(recordingKey);
        if (recordingIt == pinnedRecordingsById.end())
        {
            const auto selectedSegments = Replay::selectBinaryStoreSegments(replayBoundary.selection.filePath, selection);
            if (selectedSegments.empty())
            {
                return std::unexpected(PlacementFailure(
                    "Replay execution could not select retained segments from recording {} for interval [{} ms, {} ms]",
                    replayBoundary.selection.recordingId,
                    execution.intervalStartMs,
                    execution.intervalEndMs));
            }

            PreparedReplayRecording preparedRecording{
                .recordingId = replayBoundary.selection.recordingId,
                .filePath = replayBoundary.selection.filePath,
                .segmentIds =
                    selectedSegments | std::views::transform([](const auto& segment) { return segment.segmentId; })
                    | std::ranges::to<std::vector>()};
            recordingIt = pinnedRecordingsById.emplace(recordingKey, std::move(preparedRecording)).first;
        }

        for (const auto& materializationEdge : replayBoundary.materializationEdges)
        {
            replacements.insert_or_assign(
                materializationEdge,
                ReplaySourceReplacement{
                    .host = replayBoundary.selection.node,
                    .recordingId = replayBoundary.selection.recordingId.getRawValue(),
                    .filePath = replayBoundary.selection.filePath,
                    .segmentIds = recordingIt->second.segmentIds,
                    .intervalStartMs = execution.intervalStartMs,
                    .intervalEndMs = execution.intervalEndMs});
        }
    }

    auto replaySourcePlan = buildReplaySourcePlan(replayPlan.queryPlanRewrite.basePlan, sourceCatalog, replacements);
    if (auto validation = validatePhase5ReplayPlan(replaySourcePlan); !validation)
    {
        return std::unexpected(validation.error());
    }

    return PreparedReplayExecutionPlan{
        .replayPlan = std::move(replaySourcePlan),
        .pinnedRecordings = pinnedRecordingsById | std::views::values | std::ranges::to<std::vector>()};
}

std::expected<void, Exception> waitForReplayQueryCompletion(const SharedPtr<QueryManager>& queryManager, const DistributedQueryId& queryId)
{
    constexpr auto replayStatusPollInterval = std::chrono::milliseconds(10);
    while (true)
    {
        const auto statusResult = queryManager->status(queryId);
        if (!statusResult)
        {
            return std::unexpected(QueryStatusFailed(
                "Could not observe replay query {}: {}",
                queryId,
                joinExceptionMessages(statusResult.error())));
        }

        switch (statusResult->getGlobalQueryState())
        {
            case DistributedQueryState::Stopped:
                return {};
            case DistributedQueryState::Failed:
                if (const auto exception = statusResult->coalesceException(); exception.has_value())
                {
                    return std::unexpected(QueryStatusFailed(
                        "Replay query {} failed during execution: {}",
                        queryId,
                        exception->what()));
                }
                return std::unexpected(QueryStatusFailed("Replay query {} failed during execution", queryId));
            case DistributedQueryState::Unreachable:
                if (const auto exception = statusResult->coalesceException(); exception.has_value())
                {
                    return std::unexpected(QueryStatusFailed(
                        "Replay query {} became unreachable during execution: {}",
                        queryId,
                        exception->what()));
                }
                return std::unexpected(QueryStatusFailed("Replay query {} became unreachable during execution", queryId));
            case DistributedQueryState::Registered:
            case DistributedQueryState::Running:
            case DistributedQueryState::PartiallyStopped:
                std::this_thread::sleep_for(replayStatusPollInterval);
                break;
        }
    }
}

std::expected<void, Exception> cleanupReplayExecutionResources(
    SharedPtr<QueryManager>& queryManager,
    const std::optional<DistributedQueryId>& internalQueryId,
    const bool internalQueryStarted,
    const std::vector<PreparedReplayRecording>& pinnedRecordings)
{
    std::optional<Exception> firstException;
    if (internalQueryId.has_value())
    {
        if (internalQueryStarted)
        {
            const auto statusResult = queryManager->status(*internalQueryId);
            if (statusResult && statusResult->getGlobalQueryState() != DistributedQueryState::Stopped
                && statusResult->getGlobalQueryState() != DistributedQueryState::Failed)
            {
                const auto stopResult = queryManager->stop(*internalQueryId);
                if (!stopResult && !firstException.has_value())
                {
                    firstException = QueryStopFailed(
                        "Could not stop replay query {}: {}",
                        *internalQueryId,
                        joinExceptionMessages(stopResult.error()));
                }
            }
        }

        const auto unregisterResult = queryManager->unregister(*internalQueryId);
        if (!unregisterResult && !firstException.has_value())
        {
            firstException = QueryUnregistrationFailed(
                "Could not unregister replay query {}: {}",
                *internalQueryId,
                joinExceptionMessages(unregisterResult.error()));
        }
    }

    for (const auto& pinnedRecording : pinnedRecordings)
    {
        try
        {
            Replay::unpinBinaryStoreSegments(pinnedRecording.filePath, pinnedRecording.segmentIds);
        }
        catch (const Exception& exception)
        {
            if (!firstException.has_value())
            {
                firstException = exception;
            }
        }
    }

    if (firstException.has_value())
    {
        return std::unexpected(*firstException);
    }
    return {};
}

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

bool hasActiveReplayInsertions(const RecordingSelectionResult& selectionResult, const DistributedQueryId& queryId)
{
    const auto rewriteIt = std::ranges::find(selectionResult.activeQueryPlanRewrites, queryId.getRawValue(), &QueryRecordingPlanRewrite::queryId);
    return rewriteIt != selectionResult.activeQueryPlanRewrites.end() && !rewriteIt->insertions.empty();
}

std::expected<std::vector<RecordingEpochDeployment>, Exception> buildRecordingEpochDeployments(
    SharedPtr<QueryManager>& queryManager,
    const SharedPtr<const LegacyOptimizer>& optimizer,
    const RecordingSelectionResult& selectionResult,
    const DistributedQueryId& incomingQueryId)
{
    std::vector<RecordingEpochDeployment> deployments;
    for (const auto& queryId : collectActiveReplayRedeployQueries(selectionResult, incomingQueryId))
    {
        if (!hasActiveReplayInsertions(selectionResult, queryId))
        {
            continue;
        }

        const auto metadataIt = queryManager->getRecordingCatalog().getQueryMetadata().find(queryId);
        if (metadataIt == queryManager->getRecordingCatalog().getQueryMetadata().end())
        {
            continue;
        }

        const auto& metadata = metadataIt->second;
        if (!metadata.globalPlan.has_value() || !metadata.replaySpecification.has_value())
        {
            continue;
        }

        deployments.push_back(RecordingEpochDeployment{
            .beneficiaryQueryId = queryId,
            .epochPlan = optimizer->buildRecordingEpochWithFixedSelection(
                *metadata.globalPlan,
                metadata.replaySpecification,
                selectionResult,
                queryId.getRawValue())});
    }
    return deployments;
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

std::expected<ReplayStatementResult, Exception> QueryStatementHandler::operator()(const ReplayStatement& statement)
{
    CPPTRACE_TRY
    {
        queryManager->refreshWorkerMetrics();
        auto replayExecution = queryManager->registerReplayExecution(statement.queryId, statement.intervalStartMs, statement.intervalEndMs);
        if (!replayExecution)
        {
            return std::unexpected(replayExecution.error());
        }
        if (optimizer == nullptr || sourceCatalog == nullptr)
        {
            return std::unexpected(UnsupportedQuery("Replay execution requires optimizer and source catalog support in the frontend"));
        }

        auto execution = *replayExecution;
        std::optional<DistributedQueryId> internalQueryId;
        bool internalQueryStarted = false;
        std::vector<PreparedReplayRecording> pinnedRecordings;

        const auto persistExecution = [&]() -> std::expected<void, Exception>
        {
            return queryManager->updateReplayExecution(execution);
        };
        const auto failReplayExecution = [&](const Exception& exception) -> std::expected<ReplayStatementResult, Exception>
        {
            execution.state = ReplayExecutionState::Failed;
            execution.failureReason = std::string(exception.what());
            auto persisted = persistExecution();
            if (!persisted)
            {
                return std::unexpected(persisted.error());
            }
            return std::unexpected(exception);
        };

        execution.state = ReplayExecutionState::Preparing;
        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }

        const auto replayPlan = queryManager->getReplayPlan(execution.id);
        if (!replayPlan)
        {
            return failReplayExecution(replayPlan.error());
        }

        const auto preparedReplayPlan = buildPreparedReplayExecutionPlan(sourceCatalog, execution, *replayPlan);
        if (!preparedReplayPlan)
        {
            return failReplayExecution(preparedReplayPlan.error());
        }

        std::optional<DistributedLogicalPlan> replayDistributedPlan;
        try
        {
            replayDistributedPlan = optimizer->decomposePlacedPlan(preparedReplayPlan->replayPlan);
        }
        catch (const Exception& exception)
        {
            return failReplayExecution(exception);
        }

        try
        {
            pinnedRecordings = preparedReplayPlan->pinnedRecordings;
            execution.pinnedSegments.clear();
            for (const auto& pinnedRecording : pinnedRecordings)
            {
                const auto pinnedSegmentIds = Replay::pinBinaryStoreSegments(pinnedRecording.filePath, pinnedRecording.segmentIds);
                for (const auto segmentId : pinnedSegmentIds)
                {
                    appendPinnedSegment(execution.pinnedSegments, pinnedRecording.recordingId, segmentId);
                }
            }
        }
        catch (const Exception& exception)
        {
            execution.state = ReplayExecutionState::CleaningUp;
            if (auto persisted = persistExecution(); !persisted)
            {
                return std::unexpected(persisted.error());
            }
            const auto cleanupResult = cleanupReplayExecutionResources(queryManager, std::nullopt, false, pinnedRecordings);
            if (!cleanupResult)
            {
                execution.failureReason = std::string(cleanupResult.error().what());
                execution.state = ReplayExecutionState::Failed;
                if (auto persisted = persistExecution(); !persisted)
                {
                    return std::unexpected(persisted.error());
                }
                return std::unexpected(cleanupResult.error());
            }
            return failReplayExecution(exception);
        }

        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }

        const auto registerResult = queryManager->registerQuery(
            *replayDistributedPlan,
            std::nullopt,
            QueryRegistrationOptions{.internal = true, .includeInReplayPlanning = false});
        if (!registerResult)
        {
            execution.state = ReplayExecutionState::CleaningUp;
            if (auto persisted = persistExecution(); !persisted)
            {
                return std::unexpected(persisted.error());
            }
            const auto cleanupResult = cleanupReplayExecutionResources(queryManager, std::nullopt, false, pinnedRecordings);
            if (!cleanupResult)
            {
                execution.failureReason = std::string(cleanupResult.error().what());
                execution.state = ReplayExecutionState::Failed;
                if (auto persisted = persistExecution(); !persisted)
                {
                    return std::unexpected(persisted.error());
                }
                return std::unexpected(cleanupResult.error());
            }
            return failReplayExecution(registerResult.error());
        }

        internalQueryId = *registerResult;
        execution.internalQueryIds.push_back(*internalQueryId);
        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }

        execution.state = ReplayExecutionState::FastForwarding;
        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }

        const auto startResult = queryManager->start(*internalQueryId);
        if (!startResult)
        {
            execution.state = ReplayExecutionState::CleaningUp;
            if (auto persisted = persistExecution(); !persisted)
            {
                return std::unexpected(persisted.error());
            }
            const auto cleanupResult = cleanupReplayExecutionResources(queryManager, internalQueryId, false, pinnedRecordings);
            if (!cleanupResult)
            {
                execution.failureReason = std::string(cleanupResult.error().what());
                execution.state = ReplayExecutionState::Failed;
                if (auto persisted = persistExecution(); !persisted)
                {
                    return std::unexpected(persisted.error());
                }
                return std::unexpected(cleanupResult.error());
            }
            return failReplayExecution(QueryStartFailed(
                "Could not start replay query {}: {}",
                *internalQueryId,
                joinExceptionMessages(startResult.error())));
        }
        internalQueryStarted = true;

        execution.state = ReplayExecutionState::Emitting;
        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }

        if (const auto waitResult = waitForReplayQueryCompletion(queryManager, *internalQueryId); !waitResult)
        {
            execution.state = ReplayExecutionState::CleaningUp;
            if (auto persisted = persistExecution(); !persisted)
            {
                return std::unexpected(persisted.error());
            }
            const auto cleanupResult = cleanupReplayExecutionResources(queryManager, internalQueryId, true, pinnedRecordings);
            if (!cleanupResult)
            {
                execution.failureReason = std::string(cleanupResult.error().what());
                execution.state = ReplayExecutionState::Failed;
                if (auto persisted = persistExecution(); !persisted)
                {
                    return std::unexpected(persisted.error());
                }
                return std::unexpected(cleanupResult.error());
            }
            return failReplayExecution(waitResult.error());
        }

        execution.state = ReplayExecutionState::CleaningUp;
        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }

        const auto cleanupResult = cleanupReplayExecutionResources(queryManager, internalQueryId, true, pinnedRecordings);
        if (!cleanupResult)
        {
            execution.failureReason = std::string(cleanupResult.error().what());
            execution.state = ReplayExecutionState::Failed;
            if (auto persisted = persistExecution(); !persisted)
            {
                return std::unexpected(persisted.error());
            }
            return std::unexpected(cleanupResult.error());
        }

        execution.state = ReplayExecutionState::Done;
        execution.failureReason.reset();
        if (auto persisted = persistExecution(); !persisted)
        {
            return std::unexpected(persisted.error());
        }
        return ReplayStatementResult{execution};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
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
        const auto fullSelectionResult = distributedPlan.getRecordingSelectionResult();
        auto incomingSelectionResult = fullSelectionResult;
        std::erase_if(
            incomingSelectionResult.networkExplanations,
            [](const auto& explanation) { return !explanation.selection.coversIncomingQuery; });
        distributedPlan.setRecordingSelectionResult(std::move(incomingSelectionResult));

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
        const auto recordingEpochDeployments
            = buildRecordingEpochDeployments(queryManager, optimizer, fullSelectionResult, queryId);
        if (!recordingEpochDeployments)
        {
            (void)queryManager->unregister(queryId);
            return std::unexpected(recordingEpochDeployments.error());
        }

        std::vector<DistributedQueryId> recordingEpochQueryIds;
        recordingEpochQueryIds.reserve(recordingEpochDeployments->size());
        for (const auto& deployment : *recordingEpochDeployments)
        {
            const auto epochQueryResult = queryManager->registerRecordingEpochQuery(deployment.beneficiaryQueryId, deployment.epochPlan);
            if (!epochQueryResult)
            {
                for (const auto& epochQueryId : recordingEpochQueryIds)
                {
                    (void)queryManager->unregister(epochQueryId);
                }
                (void)queryManager->unregister(queryId);
                return std::unexpected(QueryRegistrationFailed(
                    "Could not install a recording epoch for query {}: {}",
                    deployment.beneficiaryQueryId,
                    epochQueryResult.error().what()));
            }
            recordingEpochQueryIds.push_back(*epochQueryResult);
        }

        for (const auto& epochQueryId : recordingEpochQueryIds)
        {
            const auto epochStartResult = queryManager->start(epochQueryId).transform_error(
                [&epochQueryId](auto vecOfErrors)
                {
                    return QueryStartFailed(
                        "Could not start recording epoch query {}: {}",
                        epochQueryId,
                        fmt::join(std::views::transform(vecOfErrors, [](auto exception) { return exception.what(); }), ", "));
                });
            if (!epochStartResult)
            {
                for (const auto& cleanupEpochQueryId : recordingEpochQueryIds)
                {
                    (void)queryManager->unregister(cleanupEpochQueryId);
                }
                (void)queryManager->unregister(queryId);
                return std::unexpected(epochStartResult.error());
            }
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
            for (const auto& epochQueryId : recordingEpochQueryIds)
            {
                (void)queryManager->unregister(epochQueryId);
            }
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
