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
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Time/Timestamp.hpp>
#include <val.hpp>
#include <val_concepts.hpp>

namespace NES::Nautilus
{

/// @brief The RecordBuffer is a representation of a set of records that are stored together.
/// In the common case this maps to a TupleBuffer, which stores the individual records in either a row or a columnar layout.
class RecordBuffer
{
public:
    /// @brief Creates a new record buffer with a reference to a tuple buffer
    /// @param tupleBufferRef
    explicit RecordBuffer(const nautilus::val<Memory::TupleBuffer*>& tupleBufferRef);

    void setNumRecords(const nautilus::val<uint64_t>& numRecordsValue);
    [[nodiscard]] nautilus::val<uint64_t> getNumRecords() const;

    /// Retrieve the reference to the underling memory area from the record buffer.
    nautilus::val<int8_t*> getBuffer() const;

    /// Get the reference to the underlying TupleBuffer
    const nautilus::val<Memory::TupleBuffer*>& getReference() const;

    /// Get the origin ID of the underlying tuple buffer. The origin ID is a unique identifier for the origin of the tuple buffer.
    nautilus::val<OriginId> getOriginId();
    void setOriginId(const nautilus::val<OriginId>& originId);

    void setUsedMemorySize(const nautilus::val<uint64_t>& usedMemorySize);

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

    ~RecordBuffer() = default;

private:
    nautilus::val<Memory::TupleBuffer*> tupleBufferRef;
};

}
