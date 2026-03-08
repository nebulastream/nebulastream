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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include <Time/Timestamp.hpp>

namespace NES::Replay
{

enum class RecordingLifecycleState : uint8_t
{
    Filling = 0,
    Ready = 1,
};

struct RecordingRuntimeStatus
{
    std::string recordingId;
    std::string filePath;
    RecordingLifecycleState lifecycleState = RecordingLifecycleState::Filling;
    std::optional<uint64_t> retentionWindowMs;
    std::optional<Timestamp> retainedStartWatermark;
    std::optional<Timestamp> retainedEndWatermark;
    std::optional<Timestamp> fillWatermark;
    size_t segmentCount = 0;
    size_t storageBytes = 0;
    std::optional<std::string> successorRecordingId;

    [[nodiscard]] bool operator==(const RecordingRuntimeStatus& other) const = default;
};

}
