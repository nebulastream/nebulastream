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
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <Replay/BinaryStoreFormat.hpp>
#include <Replay/RecordingRuntimeStatus.hpp>

namespace NES::Replay
{

inline constexpr std::string_view DEFAULT_RECORDING_DIRECTORY = "/tmp/REPLAY-NebulaStream/recordings";
inline constexpr std::string_view DEFAULT_RECORDING_FILE_PATH = "/tmp/REPLAY-NebulaStream/store_read_out.bin";
inline constexpr size_t DEFAULT_ESTIMATED_RECORDING_ROWS = 4096;
inline constexpr size_t MIN_RECORDING_SIZE_BYTES = 4096;

struct BinaryStoreReplaySelection
{
    std::optional<std::vector<uint64_t>> segmentIds = std::nullopt;
    std::optional<uint64_t> scanStartMs = std::nullopt;
    std::optional<uint64_t> scanEndMs = std::nullopt;

    [[nodiscard]] bool empty() const
    {
        return !segmentIds.has_value() && !scanStartMs.has_value() && !scanEndMs.has_value();
    }
};

std::recursive_mutex& getReplayStorageMutex();
[[nodiscard]] std::string getRecordingFilePath(std::string_view recordingId);
[[nodiscard]] std::string getRecordingManifestPath(std::string_view recordingFilePath);
[[nodiscard]] std::string getTimeTravelReadAliasPath();
[[nodiscard]] std::string resolveTimeTravelReadProbePath();
[[nodiscard]] bool binaryStoreManifestExists(const std::string& recordingFilePath);
[[nodiscard]] BinaryStoreManifest readBinaryStoreManifest(const std::string& recordingFilePath);
void writeBinaryStoreManifest(const std::string& recordingFilePath, const BinaryStoreManifest& manifest);
[[nodiscard]] std::vector<BinaryStoreManifestEntry>
selectBinaryStoreSegments(const std::string& recordingFilePath, const BinaryStoreReplaySelection& selection = {});
[[nodiscard]] std::vector<uint64_t> pinBinaryStoreSegments(const std::string& recordingFilePath);
[[nodiscard]] std::vector<uint64_t> pinBinaryStoreSegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds);
void unpinBinaryStoreSegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds);
[[nodiscard]] RecordingRuntimeStatus readRecordingRuntimeStatus(const std::string& recordingFilePath);
[[nodiscard]] std::vector<RecordingRuntimeStatus> getRecordingRuntimeStatuses();
[[nodiscard]] size_t getRecordingStorageBytes();
[[nodiscard]] size_t getRecordingFileCount();
[[nodiscard]] uint64_t getReplayReadBytes();
void recordReplayReadBytes(size_t bytes);
void clearReplayReadBytes();
void updateTimeTravelReadAlias(const std::string& recordingFilePath);
void clearTimeTravelReadAlias();

}
