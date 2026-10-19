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
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <stop_token>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Persists every buffer that a source emits until a completed checkpoint covers it, so that a recovered query can replay the buffers
/// whose processing the checkpoint does not reflect. Buffers are identified by persisted sequence numbers, which keep increasing across
/// restarts of the worker.
class ReplayableSourceStorage
{
public:
    explicit ReplayableSourceStorage(std::filesystem::path directory);

    void persist(SequenceNumber::Underlying persistedSequence, const TupleBuffer& buffer);

    /// Deletes the buffers up to and including the sequence, which a completed checkpoint covers.
    void prune(SequenceNumber::Underlying persistedSequence);

    /// The number of source bytes in all buffers up to and including the sequence. A source that continues reading at this offset after
    /// these buffers neither skips nor duplicates input.
    [[nodiscard]] uint64_t getByteOffsetAfter(SequenceNumber::Underlying persistedSequence) const;

    /// Emits the buffers after the sequence in sequence order and returns their number.
    uint64_t replayAfter(
        SequenceNumber::Underlying persistedSequence,
        AbstractBufferProvider& bufferProvider,
        const std::function<void(SequenceNumber::Underlying persistedSequence, TupleBuffer&& buffer)>& emit,
        const std::stop_token& stopToken) const;

private:
    struct PruneMeta
    {
        SequenceNumber::Underlying sequence = 0;
        uint64_t byteOffset = 0;
    };

    [[nodiscard]] std::filesystem::path getBufferFile(SequenceNumber::Underlying persistedSequence) const;
    void writePruneMeta() const;

    std::filesystem::path directory;
    mutable std::mutex mutex;
    PruneMeta pruned;
    /// Number of bytes of each buffer that is not pruned yet.
    std::map<SequenceNumber::Underlying, uint64_t> bufferSizes;
};

}
