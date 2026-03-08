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

#include <QueryManager/RecordingLifecycleManager.hpp>

#include <algorithm>
#include <optional>
#include <ranges>
#include <vector>

#include <Replay/ReplayStorage.hpp>

namespace NES
{
namespace
{
bool hasRuntimeObservation(const RecordingEntry& recording)
{
    return recording.segmentCount.has_value() || recording.storageBytes.has_value() || recording.retainedStartWatermark.has_value()
        || recording.retainedEndWatermark.has_value() || recording.fillWatermark.has_value();
}

bool hasCoveredRequestedRetention(const RecordingEntry& recording)
{
    if (!recording.retentionWindowMs.has_value())
    {
        return true;
    }
    if (!recording.retainedStartWatermark.has_value() || !recording.fillWatermark.has_value())
    {
        return false;
    }

    const auto retainedDuration = recording.fillWatermark->getRawValue() >= recording.retainedStartWatermark->getRawValue()
        ? recording.fillWatermark->getRawValue() - recording.retainedStartWatermark->getRawValue()
        : 0;
    return retainedDuration >= *recording.retentionWindowMs;
}

bool isReadyForActivation(const RecordingEntry& recording)
{
    return !recording.ownerQueries.empty() && recording.lifecycleState == Replay::RecordingLifecycleState::Ready;
}

std::optional<RecordingId> chooseFallbackActiveRecording(const RecordingCatalog& recordingCatalog)
{
    auto candidates = recordingCatalog.getRecordings() | std::views::values
        | std::views::filter([](const RecordingEntry& recording) { return isReadyForActivation(recording); })
        | std::ranges::to<std::vector>();
    if (candidates.empty())
    {
        return std::nullopt;
    }

    std::ranges::sort(
        candidates,
        [](const RecordingEntry& left, const RecordingEntry& right) { return left.id.getRawValue() < right.id.getRawValue(); });
    return candidates.back().id;
}
}

RecordingLifecycleManager::RecordingLifecycleManager(RecordingCatalog& recordingCatalog) : recordingCatalog(recordingCatalog) { }

void RecordingLifecycleManager::notePendingActivation(const std::optional<RecordingId>& recordingId)
{
    recordingCatalog.setPendingTimeTravelReadRecording(recordingId);
}

void RecordingLifecycleManager::reconcile()
{
    for (auto& recording : recordingCatalog.getMutableRecordings() | std::views::values)
    {
        if (recording.ownerQueries.empty())
        {
            recording.lifecycleState = Replay::RecordingLifecycleState::Draining;
            continue;
        }
        if (!hasRuntimeObservation(recording))
        {
            recording.lifecycleState = Replay::RecordingLifecycleState::Installing;
            continue;
        }
        recording.lifecycleState = hasCoveredRequestedRetention(recording) ? Replay::RecordingLifecycleState::Ready
                                                                           : Replay::RecordingLifecycleState::Filling;
    }

    if (const auto pendingRecordingId = recordingCatalog.getPendingTimeTravelReadRecordingId(); pendingRecordingId.has_value())
    {
        const auto pendingRecording = recordingCatalog.getPendingTimeTravelReadRecording();
        if (!pendingRecording.has_value() || pendingRecording->ownerQueries.empty())
        {
            recordingCatalog.setPendingTimeTravelReadRecording(std::nullopt);
        }
        else if (isReadyForActivation(*pendingRecording))
        {
            recordingCatalog.setTimeTravelReadRecording(pendingRecordingId);
            recordingCatalog.setPendingTimeTravelReadRecording(std::nullopt);
        }
    }

    if (const auto activeRecording = recordingCatalog.getTimeTravelReadRecording();
        activeRecording.has_value() && !isReadyForActivation(*activeRecording))
    {
        recordingCatalog.setTimeTravelReadRecording(std::nullopt);
    }

    if (!recordingCatalog.getTimeTravelReadRecordingId().has_value())
    {
        recordingCatalog.setTimeTravelReadRecording(chooseFallbackActiveRecording(recordingCatalog));
    }

    if (const auto activeRecording = recordingCatalog.getTimeTravelReadRecording(); activeRecording.has_value())
    {
        Replay::updateTimeTravelReadAlias(activeRecording->filePath);
    }
    else
    {
        Replay::clearTimeTravelReadAlias();
    }
}
}
