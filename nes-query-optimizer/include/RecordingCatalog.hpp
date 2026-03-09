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

#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <DistributedQuery.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RecordingSelectionResult.hpp>
#include <Replay/RecordingRuntimeStatus.hpp>
#include <Replay/ReplaySpecification.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{

struct ReplayableQueryMetadata
{
    std::optional<LogicalPlan> originalPlan;
    std::optional<LogicalPlan> globalPlan;
    std::optional<ReplaySpecification> replaySpecification;
    std::vector<RecordingId> selectedRecordings;
    std::vector<RecordingSelectionExplanation> networkExplanations;
    std::optional<QueryRecordingPlanRewrite> queryPlanRewrite = std::nullopt;
    std::vector<QueryRecordingPlanInsertion> replayBoundaries{};
};

struct RecordingEntry
{
    RecordingId id{RecordingId::INVALID};
    Host node{Host::INVALID};
    std::string filePath;
    std::string structuralFingerprint;
    std::optional<uint64_t> retentionWindowMs;
    RecordingRepresentation representation = RecordingRepresentation::BinaryStore;
    std::vector<DistributedQueryId> ownerQueries;
    std::optional<Replay::RecordingLifecycleState> lifecycleState;
    std::optional<Timestamp> retainedStartWatermark;
    std::optional<Timestamp> retainedEndWatermark;
    std::optional<Timestamp> fillWatermark;
    std::optional<size_t> segmentCount;
    std::optional<size_t> storageBytes;
    std::optional<RecordingId> successorRecordingId;
};

class RecordingCatalog
{
    std::unordered_map<DistributedQueryId, ReplayableQueryMetadata> queryMetadata;
    std::unordered_map<RecordingId, RecordingEntry> recordings;
    std::optional<RecordingId> activeTimeTravelReadRecordingId;
    std::optional<RecordingId> pendingTimeTravelReadRecordingId;

    void sanitizeTimeTravelReadRecordingIds()
    {
        if (activeTimeTravelReadRecordingId.has_value() && !recordings.contains(*activeTimeTravelReadRecordingId))
        {
            activeTimeTravelReadRecordingId.reset();
        }
        if (pendingTimeTravelReadRecordingId.has_value() && !recordings.contains(*pendingTimeTravelReadRecordingId))
        {
            pendingTimeTravelReadRecordingId.reset();
        }
    }

    void removeOwnerFromRecording(const RecordingId& recordingId, const DistributedQueryId& queryId)
    {
        const auto recordingIt = recordings.find(recordingId);
        if (recordingIt == recordings.end())
        {
            return;
        }

        auto& ownerQueries = recordingIt->second.ownerQueries;
        std::erase(ownerQueries, queryId);
    }

public:
    [[nodiscard]] const auto& getQueryMetadata() const { return queryMetadata; }
    [[nodiscard]] const auto& getRecordings() const { return recordings; }
    [[nodiscard]] auto& getMutableRecordings() { return recordings; }
    [[nodiscard]] std::optional<RecordingEntry> getRecording(const RecordingId& recordingId) const
    {
        if (const auto found = recordings.find(recordingId); found != recordings.end())
        {
            return found->second;
        }
        return std::nullopt;
    }
    [[nodiscard]] std::optional<RecordingEntry> getTimeTravelReadRecording() const
    {
        if (!activeTimeTravelReadRecordingId.has_value())
        {
            return std::nullopt;
        }
        return getRecording(*activeTimeTravelReadRecordingId);
    }
    [[nodiscard]] std::optional<RecordingId> getTimeTravelReadRecordingId() const { return activeTimeTravelReadRecordingId; }
    [[nodiscard]] std::optional<RecordingEntry> getPendingTimeTravelReadRecording() const
    {
        if (!pendingTimeTravelReadRecordingId.has_value())
        {
            return std::nullopt;
        }
        return getRecording(*pendingTimeTravelReadRecordingId);
    }
    [[nodiscard]] std::optional<RecordingId> getPendingTimeTravelReadRecordingId() const { return pendingTimeTravelReadRecordingId; }
    [[nodiscard]] std::vector<RecordingEntry> getRecordingsByStructuralFingerprint(std::string_view fingerprint) const
    {
        auto matches = recordings | std::views::values
            | std::views::filter([&](const RecordingEntry& recording) { return recording.structuralFingerprint == fingerprint; })
            | std::ranges::to<std::vector>();
        std::ranges::sort(
            matches,
            [](const RecordingEntry& left, const RecordingEntry& right)
            {
                const auto leftRetention = left.retentionWindowMs.value_or(0);
                const auto rightRetention = right.retentionWindowMs.value_or(0);
                if (leftRetention != rightRetention)
                {
                    return leftRetention < rightRetention;
                }
                return left.id.getRawValue() < right.id.getRawValue();
            });
        return matches;
    }

    void upsertQueryMetadata(DistributedQueryId queryId, ReplayableQueryMetadata metadata)
    {
        queryMetadata.insert_or_assign(std::move(queryId), std::move(metadata));
    }

    void removeQueryMetadata(const DistributedQueryId& queryId)
    {
        queryMetadata.erase(queryId);
        for (auto& recording : recordings | std::views::values)
        {
            auto& owners = recording.ownerQueries;
            std::erase(owners, queryId);
        }
        sanitizeTimeTravelReadRecordingIds();
    }

    void upsertRecording(RecordingEntry recording)
    {
        const auto [it, inserted] = recordings.try_emplace(recording.id, std::move(recording));
        if (inserted)
        {
            return;
        }

        it->second.node = std::move(recording.node);
        it->second.filePath = std::move(recording.filePath);
        if (!recording.structuralFingerprint.empty())
        {
            it->second.structuralFingerprint = std::move(recording.structuralFingerprint);
        }
        if (recording.retentionWindowMs.has_value())
        {
            it->second.retentionWindowMs = std::max(it->second.retentionWindowMs.value_or(0), *recording.retentionWindowMs);
        }
        it->second.representation = recording.representation;
        for (const auto& owner : recording.ownerQueries)
        {
            if (!std::ranges::contains(it->second.ownerQueries, owner))
            {
                it->second.ownerQueries.push_back(owner);
            }
        }
    }

    void reconcileQuerySelections(
        const DistributedQueryId& queryId,
        std::vector<RecordingId> selectedRecordings,
        std::vector<RecordingSelectionExplanation> networkExplanations)
    {
        auto& metadata = queryMetadata.at(queryId);

        std::vector<RecordingId> deduplicatedSelectedRecordings;
        deduplicatedSelectedRecordings.reserve(selectedRecordings.size());
        for (const auto& selectedRecording : selectedRecordings)
        {
            if (!std::ranges::contains(deduplicatedSelectedRecordings, selectedRecording))
            {
                deduplicatedSelectedRecordings.push_back(selectedRecording);
            }
        }

        for (const auto& previouslySelectedRecording : metadata.selectedRecordings)
        {
            if (!std::ranges::contains(deduplicatedSelectedRecordings, previouslySelectedRecording))
            {
                removeOwnerFromRecording(previouslySelectedRecording, queryId);
            }
        }

        metadata.selectedRecordings = std::move(deduplicatedSelectedRecordings);
        metadata.networkExplanations = std::move(networkExplanations);
        sanitizeTimeTravelReadRecordingIds();
    }

    void setTimeTravelReadRecording(const std::optional<RecordingId>& recordingId)
    {
        if (recordingId.has_value() && recordings.contains(*recordingId))
        {
            activeTimeTravelReadRecordingId = recordingId;
            return;
        }
        if (!recordingId.has_value())
        {
            activeTimeTravelReadRecordingId = std::nullopt;
            return;
        }
        sanitizeTimeTravelReadRecordingIds();
    }

    void setPendingTimeTravelReadRecording(const std::optional<RecordingId>& recordingId)
    {
        if (recordingId.has_value() && recordings.contains(*recordingId))
        {
            pendingTimeTravelReadRecordingId = recordingId;
            return;
        }
        if (!recordingId.has_value())
        {
            pendingTimeTravelReadRecordingId = std::nullopt;
            return;
        }
        sanitizeTimeTravelReadRecordingIds();
    }

    void reconcileWorkerRuntimeStatus(const Host& host, const std::vector<Replay::RecordingRuntimeStatus>& statuses)
    {
        std::unordered_set<std::string> reportedRecordingIds;
        reportedRecordingIds.reserve(statuses.size());
        for (const auto& status : statuses)
        {
            reportedRecordingIds.insert(status.recordingId);
            if (status.recordingId.empty())
            {
                continue;
            }

            const auto it = recordings.find(RecordingId(status.recordingId));
            if (it == recordings.end())
            {
                continue;
            }

            auto& recording = it->second;
            recording.node = host;
            recording.filePath = status.filePath;
            if (status.retentionWindowMs.has_value())
            {
                recording.retentionWindowMs = status.retentionWindowMs;
            }
            recording.lifecycleState = status.lifecycleState;
            recording.retainedStartWatermark = status.retainedStartWatermark;
            recording.retainedEndWatermark = status.retainedEndWatermark;
            recording.fillWatermark = status.fillWatermark;
            recording.segmentCount = status.segmentCount;
            recording.storageBytes = status.storageBytes;
            recording.successorRecordingId = status.successorRecordingId.has_value()
                ? std::make_optional(RecordingId(*status.successorRecordingId))
                : std::nullopt;
        }

        for (auto& recording : recordings | std::views::values)
        {
            if (recording.node != host)
            {
                continue;
            }
            if (reportedRecordingIds.contains(recording.id.getRawValue()))
            {
                continue;
            }
            recording.lifecycleState.reset();
            recording.retainedStartWatermark.reset();
            recording.retainedEndWatermark.reset();
            recording.fillWatermark.reset();
            recording.segmentCount.reset();
            recording.storageBytes.reset();
            recording.successorRecordingId.reset();
        }
    }
};

}
