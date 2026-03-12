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

#include <Replay/ReplayStorage.hpp>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <system_error>
#include <unordered_set>

#include <ErrorHandling.hpp>
#include <Util/Strings.hpp>

namespace NES::Replay
{

namespace
{
std::atomic<uint64_t> replayReadBytes{0};
std::recursive_mutex replayStorageMutex;

bool isManifestFile(const std::filesystem::path& path)
{
    return path.extension() == BINARY_STORE_MANIFEST_SUFFIX;
}

bool isCompactionTemporaryFile(const std::filesystem::path& path)
{
    constexpr std::string_view suffix = ".gc.tmp";
    const auto pathString = path.string();
    return pathString.size() >= suffix.size() && pathString.ends_with(suffix);
}

bool isRecordingPayloadFile(const std::filesystem::path& path)
{
    return !isManifestFile(path) && !isCompactionTemporaryFile(path);
}

bool overlapsReplayInterval(
    const BinaryStoreManifestEntry& segment, const std::optional<uint64_t> scanStartMs, const std::optional<uint64_t> scanEndMs)
{
    if (scanStartMs.has_value() && segment.maxWatermark < *scanStartMs)
    {
        return false;
    }
    if (scanEndMs.has_value() && segment.minWatermark > *scanEndMs)
    {
        return false;
    }
    return true;
}

std::string missingSegmentIdsMessage(const std::vector<uint64_t>& missingSegmentIds)
{
    std::ostringstream message;
    for (size_t index = 0; index < missingSegmentIds.size(); ++index)
    {
        if (index > 0)
        {
            message << ", ";
        }
        message << missingSegmentIds[index];
    }
    return message.str();
}

RecordingLifecycleState deriveRecordingLifecycleState(const BinaryStoreManifest& manifest)
{
    if (manifest.segments.empty())
    {
        return RecordingLifecycleState::Installing;
    }
    if (!manifest.retentionWindowMs.has_value())
    {
        return RecordingLifecycleState::Ready;
    }

    const auto retainedStart = manifest.retainedStartWatermark();
    const auto fillWatermark = manifest.fillWatermark();
    if (!retainedStart.has_value() || !fillWatermark.has_value())
    {
        return RecordingLifecycleState::Filling;
    }

    const auto retainedDurationMs = fillWatermark->getRawValue() >= retainedStart->getRawValue()
        ? fillWatermark->getRawValue() - retainedStart->getRawValue()
        : 0;
    return retainedDurationMs >= *manifest.retentionWindowMs ? RecordingLifecycleState::Ready : RecordingLifecycleState::Filling;
}

void writeBinaryStoreManifestUnlocked(const std::string& manifestPath, const BinaryStoreManifest& manifest)
{
    std::ofstream manifestOutput(manifestPath, std::ios::trunc);
    if (!manifestOutput)
    {
        throw CannotOpenSink("Could not create binary store manifest {}", manifestPath);
    }

    manifestOutput << BINARY_STORE_MANIFEST_MAGIC << '\n';
    if (manifest.retentionWindowMs.has_value())
    {
        manifestOutput << BINARY_STORE_MANIFEST_METADATA_PREFIX << BINARY_STORE_MANIFEST_RETENTION_WINDOW_KEY << '='
                       << *manifest.retentionWindowMs << '\n';
    }
    if (manifest.successorRecordingId.has_value())
    {
        manifestOutput << BINARY_STORE_MANIFEST_METADATA_PREFIX << BINARY_STORE_MANIFEST_SUCCESSOR_RECORDING_KEY << '='
                       << *manifest.successorRecordingId << '\n';
    }
    for (const auto& entry : manifest.segments)
    {
        manifestOutput << entry.segmentId << ' ' << entry.payloadOffset << ' ' << entry.storedSizeBytes << ' ' << entry.logicalSizeBytes
                       << ' ' << entry.minWatermark << ' ' << entry.maxWatermark << ' ' << entry.pinCount << '\n';
    }
    if (!manifestOutput)
    {
        throw CannotOpenSink("Failed to write binary store manifest {}", manifestPath);
    }
}

void parseManifestMetadataLine(const std::string& line, BinaryStoreManifest& manifest, const std::string& manifestPath)
{
    if (!line.starts_with(BINARY_STORE_MANIFEST_METADATA_PREFIX))
    {
        return;
    }

    const auto assignment = line.substr(BINARY_STORE_MANIFEST_METADATA_PREFIX.size());
    const auto separatorPosition = assignment.find('=');
    if (separatorPosition == std::string::npos)
    {
        throw CannotOpenSink("Invalid binary store manifest metadata in {}", manifestPath);
    }

    const auto key = assignment.substr(0, separatorPosition);
    const auto value = assignment.substr(separatorPosition + 1);
    if (key == BINARY_STORE_MANIFEST_RETENTION_WINDOW_KEY)
    {
        if (value.empty())
        {
            manifest.retentionWindowMs.reset();
            return;
        }
        const auto retentionWindowMs = from_chars<uint64_t>(value);
        if (!retentionWindowMs.has_value())
        {
            throw CannotOpenSink("Invalid binary store retention metadata in {}", manifestPath);
        }
        manifest.retentionWindowMs = *retentionWindowMs;
        return;
    }

    if (key == BINARY_STORE_MANIFEST_SUCCESSOR_RECORDING_KEY)
    {
        manifest.successorRecordingId = value.empty() ? std::nullopt : std::make_optional(value);
        return;
    }

    throw CannotOpenSink("Unknown binary store manifest metadata key {} in {}", key, manifestPath);
}

template <typename Projection>
size_t accumulateRecordingDirectory(Projection projection)
{
    const auto recordingDirectory = std::filesystem::path(DEFAULT_RECORDING_DIRECTORY);
    std::error_code ec;
    if (!std::filesystem::exists(recordingDirectory, ec) || ec)
    {
        return 0;
    }

    uintmax_t total = 0;
    for (std::filesystem::recursive_directory_iterator iterator(
             recordingDirectory, std::filesystem::directory_options::skip_permission_denied, ec),
         end;
         iterator != end;
         iterator.increment(ec))
    {
        if (ec)
        {
            ec.clear();
            continue;
        }

        total += projection(*iterator, ec);
        if (ec)
        {
            ec.clear();
        }
    }

    return static_cast<size_t>(std::min<uintmax_t>(total, std::numeric_limits<size_t>::max()));
}

BinaryStoreManifest readBinaryStoreManifestUnlocked(const std::string& recordingFilePath)
{
    const auto manifestPath = getRecordingManifestPath(recordingFilePath);
    std::ifstream input(manifestPath);
    if (!input)
    {
        return {};
    }

    std::string header;
    std::getline(input, header);
    if (!input || header != BINARY_STORE_MANIFEST_MAGIC)
    {
        throw CannotOpenSink("Invalid binary store manifest header in {}", manifestPath);
    }

    BinaryStoreManifest manifest;
    std::string line;
    while (std::getline(input, line))
    {
        if (line.empty())
        {
            continue;
        }
        if (line.starts_with(BINARY_STORE_MANIFEST_METADATA_PREFIX))
        {
            parseManifestMetadataLine(line, manifest, manifestPath);
            continue;
        }

        std::istringstream lineStream(line);
        BinaryStoreManifestEntry entry;
        if (!(lineStream >> entry.segmentId >> entry.payloadOffset >> entry.storedSizeBytes >> entry.logicalSizeBytes >> entry.minWatermark
              >> entry.maxWatermark))
        {
            throw CannotOpenSink("Invalid binary store manifest entry in {}", manifestPath);
        }
        if (!(lineStream >> entry.pinCount))
        {
            lineStream.clear();
            entry.pinCount = 0;
        }
        manifest.segments.push_back(entry);
    }

    return manifest;
}
}

std::recursive_mutex& getReplayStorageMutex()
{
    return replayStorageMutex;
}

std::string getRecordingFilePath(std::string_view recordingId)
{
    return (std::filesystem::path(DEFAULT_RECORDING_DIRECTORY) / std::string(recordingId)).concat(".bin").string();
}

std::string getRecordingManifestPath(std::string_view recordingFilePath)
{
    return std::string(recordingFilePath) + std::string(BINARY_STORE_MANIFEST_SUFFIX);
}

std::string getTimeTravelReadAliasPath()
{
    return std::string(DEFAULT_RECORDING_FILE_PATH);
}

std::string resolveTimeTravelReadProbePath()
{
    const auto aliasPath = std::filesystem::path(getTimeTravelReadAliasPath());
    std::error_code ec;
    if (std::filesystem::is_symlink(aliasPath, ec))
    {
        auto target = std::filesystem::read_symlink(aliasPath, ec);
        if (!ec)
        {
            if (target.is_relative())
            {
                return (aliasPath.parent_path() / target).lexically_normal().string();
            }
            return target.string();
        }
    }
    return aliasPath.string();
}

bool binaryStoreManifestExists(const std::string& recordingFilePath)
{
    const auto manifestPath = getRecordingManifestPath(recordingFilePath);
    std::error_code errorCode;
    return std::filesystem::is_regular_file(manifestPath, errorCode) && !errorCode;
}

BinaryStoreManifest readBinaryStoreManifest(const std::string& recordingFilePath)
{
    std::lock_guard lock(replayStorageMutex);
    return readBinaryStoreManifestUnlocked(recordingFilePath);
}

void writeBinaryStoreManifest(const std::string& recordingFilePath, const BinaryStoreManifest& manifest)
{
    std::lock_guard lock(replayStorageMutex);
    writeBinaryStoreManifestUnlocked(getRecordingManifestPath(recordingFilePath), manifest);
}

std::vector<BinaryStoreManifestEntry>
selectBinaryStoreSegments(const std::string& recordingFilePath, const BinaryStoreReplaySelection& selection)
{
    if (selection.scanStartMs.has_value() && selection.scanEndMs.has_value() && *selection.scanStartMs > *selection.scanEndMs)
    {
        throw CannotOpenSink(
            "Invalid replay scan interval for {}: start {} ms must be less than or equal to end {} ms",
            recordingFilePath,
            *selection.scanStartMs,
            *selection.scanEndMs);
    }

    std::lock_guard lock(replayStorageMutex);
    const auto manifest = readBinaryStoreManifestUnlocked(recordingFilePath);
    if (manifest.segments.empty())
    {
        return {};
    }
    if (selection.empty())
    {
        return manifest.segments;
    }

    std::unordered_set<uint64_t> requestedSegmentIds;
    if (selection.segmentIds.has_value())
    {
        requestedSegmentIds = selection.segmentIds.value() | std::ranges::to<std::unordered_set<uint64_t>>();
        std::vector<uint64_t> missingSegmentIds;
        for (const auto requestedSegmentId : requestedSegmentIds)
        {
            const auto present = std::ranges::any_of(
                manifest.segments, [requestedSegmentId](const auto& segment) { return segment.segmentId == requestedSegmentId; });
            if (!present)
            {
                missingSegmentIds.push_back(requestedSegmentId);
            }
        }
        if (!missingSegmentIds.empty())
        {
            std::ranges::sort(missingSegmentIds);
            throw CannotOpenSink(
                "Recording {} does not contain requested replay segment ids [{}]",
                recordingFilePath,
                missingSegmentIdsMessage(missingSegmentIds));
        }
    }

    std::vector<BinaryStoreManifestEntry> selectedSegments;
    selectedSegments.reserve(manifest.segments.size());
    for (const auto& segment : manifest.segments)
    {
        if (!requestedSegmentIds.empty() && !requestedSegmentIds.contains(segment.segmentId))
        {
            continue;
        }
        if (!overlapsReplayInterval(segment, selection.scanStartMs, selection.scanEndMs))
        {
            continue;
        }
        selectedSegments.push_back(segment);
    }
    return selectedSegments;
}

std::vector<uint64_t> pinBinaryStoreSegments(const std::string& recordingFilePath)
{
    std::lock_guard lock(replayStorageMutex);
    auto manifest = readBinaryStoreManifestUnlocked(recordingFilePath);
    if (manifest.segments.empty())
    {
        return {};
    }

    std::vector<uint64_t> pinnedSegmentIds;
    pinnedSegmentIds.reserve(manifest.segments.size());
    for (auto& segment : manifest.segments)
    {
        ++segment.pinCount;
        pinnedSegmentIds.push_back(segment.segmentId);
    }
    writeBinaryStoreManifestUnlocked(getRecordingManifestPath(recordingFilePath), manifest);
    return pinnedSegmentIds;
}

std::vector<uint64_t> pinBinaryStoreSegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds)
{
    if (segmentIds.empty())
    {
        return {};
    }

    std::lock_guard lock(replayStorageMutex);
    auto manifest = readBinaryStoreManifestUnlocked(recordingFilePath);
    if (manifest.segments.empty())
    {
        return {};
    }

    const auto requestedSegmentIds = segmentIds | std::ranges::to<std::unordered_set<uint64_t>>();
    std::vector<uint64_t> pinnedSegmentIds;
    pinnedSegmentIds.reserve(requestedSegmentIds.size());
    for (auto& segment : manifest.segments)
    {
        if (!requestedSegmentIds.contains(segment.segmentId))
        {
            continue;
        }
        ++segment.pinCount;
        pinnedSegmentIds.push_back(segment.segmentId);
    }

    if (pinnedSegmentIds.size() != requestedSegmentIds.size())
    {
        const auto pinnedSet = pinnedSegmentIds | std::ranges::to<std::unordered_set<uint64_t>>();
        std::vector<uint64_t> missingSegmentIds;
        missingSegmentIds.reserve(requestedSegmentIds.size() - pinnedSegmentIds.size());
        for (const auto requestedSegmentId : requestedSegmentIds)
        {
            if (!pinnedSet.contains(requestedSegmentId))
            {
                missingSegmentIds.push_back(requestedSegmentId);
            }
        }
        std::ranges::sort(missingSegmentIds);
        throw CannotOpenSink(
            "Recording {} does not contain requested replay segment ids [{}]",
            recordingFilePath,
            missingSegmentIdsMessage(missingSegmentIds));
    }

    writeBinaryStoreManifestUnlocked(getRecordingManifestPath(recordingFilePath), manifest);
    return pinnedSegmentIds;
}

void unpinBinaryStoreSegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds)
{
    if (segmentIds.empty())
    {
        return;
    }

    std::lock_guard lock(replayStorageMutex);
    auto manifest = readBinaryStoreManifestUnlocked(recordingFilePath);
    if (manifest.segments.empty())
    {
        return;
    }

    const auto requestedSegmentIds = segmentIds | std::ranges::to<std::unordered_set<uint64_t>>();
    bool changed = false;
    for (auto& segment : manifest.segments)
    {
        if (!requestedSegmentIds.contains(segment.segmentId) || segment.pinCount == 0)
        {
            continue;
        }
        --segment.pinCount;
        changed = true;
    }

    if (changed)
    {
        writeBinaryStoreManifestUnlocked(getRecordingManifestPath(recordingFilePath), manifest);
    }
}

RecordingRuntimeStatus readRecordingRuntimeStatus(const std::string& recordingFilePath)
{
    const auto manifest = readBinaryStoreManifest(recordingFilePath);
    const auto recordingPath = std::filesystem::path(recordingFilePath);
    std::error_code errorCode;
    const auto storageBytes = std::filesystem::is_regular_file(recordingPath, errorCode) && !errorCode
        ? static_cast<size_t>(std::filesystem::file_size(recordingPath, errorCode))
        : 0U;

    return RecordingRuntimeStatus{
        .recordingId = recordingPath.stem().string(),
        .filePath = recordingFilePath,
        .lifecycleState = deriveRecordingLifecycleState(manifest),
        .retentionWindowMs = manifest.retentionWindowMs,
        .retainedStartWatermark = manifest.retainedStartWatermark(),
        .retainedEndWatermark = manifest.retainedEndWatermark(),
        .fillWatermark = manifest.fillWatermark(),
        .segmentCount = manifest.segments.size(),
        .storageBytes = storageBytes,
        .successorRecordingId = manifest.successorRecordingId};
}

std::vector<RecordingRuntimeStatus> getRecordingRuntimeStatuses()
{
    std::vector<RecordingRuntimeStatus> statuses;
    const auto recordingDirectory = std::filesystem::path(DEFAULT_RECORDING_DIRECTORY);
    std::error_code errorCode;
    if (!std::filesystem::exists(recordingDirectory, errorCode) || errorCode)
    {
        return statuses;
    }

    for (std::filesystem::recursive_directory_iterator iterator(
             recordingDirectory, std::filesystem::directory_options::skip_permission_denied, errorCode),
         end;
         iterator != end;
         iterator.increment(errorCode))
    {
        if (errorCode)
        {
            errorCode.clear();
            continue;
        }
        if (!iterator->is_regular_file(errorCode) || errorCode)
        {
            errorCode.clear();
            continue;
        }
        if (!isRecordingPayloadFile(iterator->path()))
        {
            continue;
        }
        statuses.push_back(readRecordingRuntimeStatus(iterator->path().string()));
    }

    std::ranges::sort(
        statuses,
        [](const RecordingRuntimeStatus& left, const RecordingRuntimeStatus& right)
        {
            if (left.recordingId != right.recordingId)
            {
                return left.recordingId < right.recordingId;
            }
            return left.filePath < right.filePath;
        });
    return statuses;
}

size_t getRecordingStorageBytes()
{
    return accumulateRecordingDirectory(
        [](const std::filesystem::directory_entry& entry, std::error_code& ec) -> uintmax_t
        {
            if (!entry.is_regular_file(ec))
            {
                return 0;
            }
            if (!isRecordingPayloadFile(entry.path()))
            {
                return 0;
            }
            return entry.file_size(ec);
        });
}

size_t getRecordingFileCount()
{
    return accumulateRecordingDirectory(
        [](const std::filesystem::directory_entry& entry, std::error_code& ec) -> uintmax_t
        {
            if (entry.is_regular_file(ec))
            {
                if (!isRecordingPayloadFile(entry.path()))
                {
                    return 0;
                }
                return 1;
            }
            return 0;
        });
}

uint64_t getReplayReadBytes()
{
    return replayReadBytes.load();
}

void recordReplayReadBytes(const size_t bytes)
{
    replayReadBytes.fetch_add(bytes);
}

void clearReplayReadBytes()
{
    replayReadBytes.store(0);
}

void updateTimeTravelReadAlias(const std::string& recordingFilePath)
{
    const auto aliasPath = std::filesystem::path(getTimeTravelReadAliasPath());
    const auto targetPath = std::filesystem::path(recordingFilePath);

    std::error_code ec;
    if (!aliasPath.parent_path().empty())
    {
        std::filesystem::create_directories(aliasPath.parent_path(), ec);
        ec.clear();
    }
    if (!targetPath.parent_path().empty())
    {
        std::filesystem::create_directories(targetPath.parent_path(), ec);
        ec.clear();
    }

    std::filesystem::remove(aliasPath, ec);
    ec.clear();
    std::filesystem::create_symlink(targetPath, aliasPath, ec);
}

void clearTimeTravelReadAlias()
{
    std::error_code ec;
    std::filesystem::remove(std::filesystem::path(getTimeTravelReadAliasPath()), ec);
}

}
