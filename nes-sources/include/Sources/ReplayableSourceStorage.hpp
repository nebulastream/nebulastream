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
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stop_token>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
namespace detail
{
using EmitFn = std::function<void(TupleBuffer, bool addBufferMetadata)>;
}

/// Persists source buffers to disk so they can be replayed after a checkpoint recovery.
/// Stored buffers are pruned once a checkpoint callback confirms they are covered by a new checkpoint.
class ReplayableSourceStorage
{
public:
    ReplayableSourceStorage(OriginId originId, std::filesystem::path storageDirectory, PhysicalSourceId physicalSourceId);

    /// Persist a buffer to disk. Expects metadata (seq/chunk/last) to already be set on the buffer.
    void persistBuffer(const TupleBuffer& buffer);

    /// Record that a checkpoint covers all sequences up to and including \p sequenceNumber.
    /// Also prunes persisted buffers that are now covered.
    void markCheckpoint(SequenceNumber sequenceNumber);

    /// Replay all persisted buffers that are newer than the last checkpoint.
    /// Uses the provided emit function to push data back into the pipeline without modifying metadata.
    /// Returns the last replayed sequence number if any buffer was replayed.
    std::optional<SequenceNumber> replayPending(
        AbstractBufferProvider& bufferProvider,
        const detail::EmitFn& emit,
        const std::stop_token& stopToken);

    /// Load previously persisted checkpoint metadata if present.
    std::optional<SequenceNumber::Underlying> loadLastCheckpointedSequence();

private:
    #pragma pack(push, 1)
    struct FileHeader
    {
        uint64_t sequence{0};
        uint64_t chunk{0};
        uint8_t lastChunk{0};
        uint64_t dataSize{0};
    };
    #pragma pack(pop)

    void ensureDirectory();
    void writeCheckpointMeta(SequenceNumber sequenceNumber);
    void pruneCoveredFiles(SequenceNumber::Underlying checkpointSequence);
    std::vector<std::pair<SequenceNumber::Underlying, std::filesystem::path>> listPersistedFiles();

    OriginId originId;
    PhysicalSourceId physicalSourceId;
    std::filesystem::path storageDir;
    std::filesystem::path metaFilePath;
    std::mutex mutex;
    std::optional<SequenceNumber::Underlying> lastCheckpointed;
};

}
