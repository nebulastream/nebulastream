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

Replay::RecordingLifecycleState deriveRuntimeLifecycleStateFromObservations(const RecordingEntry& recording)
{
    if (recording.segmentCount.value_or(0) == 0)
    {
        return Replay::RecordingLifecycleState::Installing;
    }
    return hasCoveredRequestedRetention(recording) ? Replay::RecordingLifecycleState::Ready : Replay::RecordingLifecycleState::Filling;
}

Replay::RecordingLifecycleState runtimeLifecycleState(const RecordingEntry& recording)
{
    if (recording.lifecycleState.has_value())
    {
        switch (*recording.lifecycleState)
        {
            case Replay::RecordingLifecycleState::Installing:
            case Replay::RecordingLifecycleState::Filling:
            case Replay::RecordingLifecycleState::Ready:
                return *recording.lifecycleState;
            case Replay::RecordingLifecycleState::New:
            case Replay::RecordingLifecycleState::Draining:
            case Replay::RecordingLifecycleState::Deleted:
                break;
        }
    }
    return deriveRuntimeLifecycleStateFromObservations(recording);
}

bool successorStillWarming(const RecordingEntry& recording, const RecordingCatalog& recordingCatalog)
{
    if (!recording.successorRecordingId.has_value())
    {
        return false;
    }

    const auto successor = recordingCatalog.getRecording(*recording.successorRecordingId);
    return successor.has_value() && !successor->ownerQueries.empty()
        && successor->lifecycleState != Replay::RecordingLifecycleState::Ready
        && successor->lifecycleState != Replay::RecordingLifecycleState::Deleted;
}

Replay::RecordingLifecycleState reconcileLifecycleState(const RecordingEntry& recording, const RecordingCatalog& recordingCatalog)
{
    if (!recording.ownerQueries.empty())
    {
        if (!hasRuntimeObservation(recording))
        {
            return Replay::RecordingLifecycleState::Installing;
        }
        return runtimeLifecycleState(recording);
    }

    if (!hasRuntimeObservation(recording))
    {
        return Replay::RecordingLifecycleState::Deleted;
    }

    if (successorStillWarming(recording, recordingCatalog))
    {
        return runtimeLifecycleState(recording);
    }

    return Replay::RecordingLifecycleState::Draining;
}

bool isReadyForActivation(const RecordingEntry& recording)
{
    return !recording.ownerQueries.empty() && recording.lifecycleState == Replay::RecordingLifecycleState::Ready;
}

bool isUsableActiveRecording(const RecordingEntry& recording)
{
    return recording.lifecycleState == Replay::RecordingLifecycleState::Ready;
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
        recording.lifecycleState = reconcileLifecycleState(recording, recordingCatalog);
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
        activeRecording.has_value() && !isUsableActiveRecording(*activeRecording))
    {
        recordingCatalog.setTimeTravelReadRecording(std::nullopt);
    }

    if (!recordingCatalog.getTimeTravelReadRecordingId().has_value())
    {
        recordingCatalog.setTimeTravelReadRecording(chooseFallbackActiveRecording(recordingCatalog));
    }

}
}
