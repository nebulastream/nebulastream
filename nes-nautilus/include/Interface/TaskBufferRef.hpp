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
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <val.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// @brief Wraps a NautilusBuffer and provides access to it and its metadata
class TaskBufferRef
{
public:
    /// Wraps a NautilusBuffer (owned or borrowed)
    explicit TaskBufferRef(NautilusBuffer buffer);

    void setNumRecords(const nautilus::val<uint64_t>& numRecordsValue);
    [[nodiscard]] nautilus::val<uint64_t> getNumRecords() const;

    /// Get the underlying NautilusBuffer, e.g. to pass it into a `nautilus::invoke` via asArg()
    NautilusBuffer& getBuffer();
    [[nodiscard]] const NautilusBuffer& getBuffer() const;

    /// Get the origin ID of the underlying tuple buffer. The origin ID is a unique identifier for the origin of the tuple buffer.
    nautilus::val<OriginId> getOriginId();
    void setOriginId(const nautilus::val<OriginId>& originId);

    /// Get the sequence number of the underlying tuple buffer. The sequence number is a monotonically increasing identifier for tuple buffers
    /// from the same origin.
    nautilus::val<SequenceNumber> getSequenceNumber();
    void setSequenceNumber(const nautilus::val<SequenceNumber>& seqNumber);

    /// Sets the chunk number of the underlying tuple buffer. The chunk number is a monotonically increasing identifier for chunks of a sequence number.
    void setChunkNumber(const nautilus::val<ChunkNumber>& chunkNumber);
    nautilus::val<ChunkNumber> getChunkNumber();
    void setLastChunk(const nautilus::val<bool>& isLastChunk);
    nautilus::val<bool> isLastChunk();

    ///  Get the watermark timestamp of the underlying tuple buffer. The watermark timestamp is a point in time that guarantees no records
    ///  with a lower timestamp will be received.
    nautilus::val<Timestamp> getWatermarkTs();
    void setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs);

    /// Get the creation timestamp of the underlying tuple buffer. The creation timestamp is the point in time when the tuple buffer was created.
    nautilus::val<Timestamp> getCreatingTs();
    void setCreationTs(const nautilus::val<Timestamp>& creationTs);

    ~TaskBufferRef() = default;

private:
    NautilusBuffer buffer;
};

}
