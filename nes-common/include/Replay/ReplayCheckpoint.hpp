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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryId.hpp>

namespace NES
{

struct ReplayCheckpointBoundary
{
    OperatorId parentOperatorId{INVALID_OPERATOR_ID};
    size_t childIndex = 0;

    [[nodiscard]] bool operator==(const ReplayCheckpointBoundary& other) const = default;
};

struct ReplayCheckpointRequirement
{
    std::string host;
    std::string replayRestoreFingerprint;

    [[nodiscard]] bool operator==(const ReplayCheckpointRequirement& other) const = default;
};

struct ReplayCheckpointReference
{
    std::string host;
    std::string bundleName;
    std::string planFingerprint;
    std::string replayRestoreFingerprint;
    uint64_t checkpointWatermarkMs = 0;

    [[nodiscard]] bool operator==(const ReplayCheckpointReference& other) const = default;
};

struct ReplayCheckpointRegistration
{
    std::optional<ReplayCheckpointReference> restoreCheckpoint = std::nullopt;
    std::optional<std::string> replayRestoreFingerprint = std::nullopt;

    [[nodiscard]] bool operator==(const ReplayCheckpointRegistration& other) const = default;
};

struct ReplayCheckpointStatus
{
    QueryId queryId = INVALID_QUERY_ID;
    std::string bundleName;
    std::string planFingerprint;
    std::string replayRestoreFingerprint;
    uint64_t checkpointWatermarkMs = 0;

    [[nodiscard]] bool operator==(const ReplayCheckpointStatus& other) const = default;
};

}
