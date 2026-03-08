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
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>
#include <DistributedQuery.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RecordingSelectionResult.hpp>
#include <Replay/ReplaySpecification.hpp>

namespace NES
{

struct ReplayableQueryMetadata
{
    std::optional<LogicalPlan> globalPlan;
    std::optional<ReplaySpecification> replaySpecification;
    std::vector<RecordingId> selectedRecordings;
    std::vector<RecordingSelectionExplanation> networkExplanations;
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
};

class RecordingCatalog
{
    std::unordered_map<DistributedQueryId, ReplayableQueryMetadata> queryMetadata;
    std::unordered_map<RecordingId, RecordingEntry> recordings;
    std::optional<RecordingId> timeTravelReadRecordingId;

    void refreshTimeTravelReadRecording()
    {
        if (timeTravelReadRecordingId.has_value() && !recordings.contains(*timeTravelReadRecordingId))
        {
            timeTravelReadRecordingId = recordings.empty() ? std::nullopt : std::optional(recordings.begin()->first);
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
        if (ownerQueries.empty())
        {
            recordings.erase(recordingIt);
        }
    }

public:
    [[nodiscard]] const auto& getQueryMetadata() const { return queryMetadata; }
    [[nodiscard]] const auto& getRecordings() const { return recordings; }
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
        if (!timeTravelReadRecordingId.has_value())
        {
            return std::nullopt;
        }
        return getRecording(*timeTravelReadRecordingId);
    }
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
        for (auto it = recordings.begin(); it != recordings.end();)
        {
            auto& owners = it->second.ownerQueries;
            std::erase(owners, queryId);
            if (owners.empty())
            {
                it = recordings.erase(it);
            }
            else
            {
                ++it;
            }
        }
        refreshTimeTravelReadRecording();
    }

    void upsertRecording(RecordingEntry recording)
    {
        timeTravelReadRecordingId = recording.id;
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
        refreshTimeTravelReadRecording();
    }

    void setTimeTravelReadRecording(const std::optional<RecordingId>& recordingId)
    {
        if (recordingId.has_value() && recordings.contains(*recordingId))
        {
            timeTravelReadRecordingId = recordingId;
            return;
        }
        if (!recordingId.has_value())
        {
            timeTravelReadRecordingId = std::nullopt;
            return;
        }
        refreshTimeTravelReadRecording();
    }
};

}
