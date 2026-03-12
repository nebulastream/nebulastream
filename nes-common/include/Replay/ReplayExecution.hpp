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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <Identifiers/NESStrongType.hpp>
#include <QueryId.hpp>
#include <Replay/ReplayCheckpoint.hpp>

namespace NES
{

using ReplayExecutionId = NESStrongUUIDType<struct ReplayExecutionId_>;

enum class ReplayExecutionState : uint8_t
{
    Planned = 0,
    Preparing = 1,
    FastForwarding = 2,
    Emitting = 3,
    CleaningUp = 4,
    Done = 5,
    Failed = 6,
};

struct ReplayPinnedSegment
{
    std::string recordingId;
    uint64_t segmentId = 0;

    [[nodiscard]] bool operator==(const ReplayPinnedSegment& other) const = default;
};

struct ReplayExecution
{
    ReplayExecutionId id = ReplayExecutionId(ReplayExecutionId::INVALID);
    DistributedQueryId queryId = DistributedQueryId(DistributedQueryId::INVALID);
    uint64_t intervalStartMs = 0;
    uint64_t intervalEndMs = 0;
    ReplayExecutionState state = ReplayExecutionState::Planned;
    uint64_t warmupStartMs = 0;
    std::vector<ReplayCheckpointReference> selectedCheckpoints;
    std::vector<std::string> selectedRecordingIds;
    std::vector<ReplayPinnedSegment> pinnedSegments;
    std::vector<DistributedQueryId> internalQueryIds;
    std::optional<std::string> failureReason;

    [[nodiscard]] bool operator==(const ReplayExecution& other) const = default;
};

}
