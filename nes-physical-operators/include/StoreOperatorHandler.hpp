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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <mutex>
#include <string>
#include <vector>

#include <Replay/BinaryStoreFormat.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include "Runtime/QueryTerminationType.hpp"

namespace NES
{

/// POSIX-based writer operator handler for the Store operator.
class StoreOperatorHandler final : public OperatorHandler
{
public:
    struct Config
    {
        std::string filePath;
        bool append{true};
        bool header{true};
        uint32_t chunkMinBytes{65536};
        bool directIO{false};
        uint32_t fdatasyncInterval{0};
        Replay::BinaryStoreCompressionCodec compression{Replay::BinaryStoreCompressionCodec::None};
        int32_t compressionLevel{3};
        std::optional<uint64_t> retentionWindowMs;
        std::string schemaText;
    };

    explicit StoreOperatorHandler(Config cfg);
    ~StoreOperatorHandler() override = default;

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void ensureHeader(PipelineExecutionContext& pec);

    void append(const uint8_t* data, size_t len, Timestamp watermark);

private:
    void flushPendingSegment();
    void flushRawManifestSegment();
    void resetCurrentSegment();
    void updateCurrentSegmentWatermark(Timestamp watermark);
    void appendManifestEntry(uint64_t payloadOffset, uint64_t storedSizeBytes, uint64_t logicalSizeBytes);
    void maybeGarbageCollectExpiredSegments(Timestamp currentWatermark);
    void rewriteRecording(
        const Replay::BinaryStoreManifest& originalManifest, const Replay::BinaryStoreManifest& retainedManifest);
    void initializeManifestState(bool truncateManifest);
    void openFile(bool truncateExisting);
    void validateExistingFile() const;
    [[nodiscard]] uint64_t writeAtTail(const uint8_t* data, size_t len);
    void writeHeaderIfNeeded();
    static uint64_t fnv1a64(const char* data, size_t len);

    int fd{-1};
    std::atomic<uint64_t> tail{0};
    std::atomic<bool> headerWritten{false};
    Config config;
    std::vector<uint8_t> pendingSegment;
    std::mutex pendingSegmentMutex;
    uint64_t nextSegmentId{0};
    uint64_t currentSegmentOffset{0};
    uint64_t currentSegmentStoredSizeBytes{0};
    uint64_t currentSegmentLogicalSizeBytes{0};
    std::optional<Timestamp> currentSegmentMinWatermark;
    std::optional<Timestamp> currentSegmentMaxWatermark;
    uint64_t writesSinceSync{0};
    static constexpr uint64_t FNVOffsetBasis = 14695981039346656037ULL;
    static constexpr uint64_t FNVPrime = 1099511628211ULL;
};

}
