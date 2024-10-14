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

#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Nautilus
{
class Record;

/**
 * @brief The RecordBuffer is a representation of a set of records that are stored together.
 * In the common case this maps to a TupleBuffer, which stores the individual records in either a row or a columnar layout.
 */
class RecordBuffer
{
public:
    /**
     * @brief Creates a new record buffer with a reference to a tuple buffer
     * @param tupleBufferRef
     */
    explicit RecordBuffer(const nautilus::val<Memory::TupleBuffer*>& tupleBufferRef);

    nautilus::val<uint64_t> getNumRecords();

    /// Retrieve the reference to the underling memory area from the record buffer.
    nautilus::val<int8_t*> getBuffer() const;

    /// Get the reference to the underlying TupleBuffer
    const nautilus::val<Memory::TupleBuffer*>& getReference() const;

    void setNumRecords(const nautilus::val<uint64_t>& numRecordsValue);

    /// Get the origin ID of the underlying tuple buffer. The origin ID is a unique identifier for the origin of the tuple buffer.
    nautilus::val<uint64_t> getOriginId();
    void setOriginId(const nautilus::val<uint64_t>& originId);

    /// Get the sequence number of the underlying tuple buffer. The sequence number is a monotonically increasing identifier for tuple buffers
    /// from the same origin.
    nautilus::val<uint64_t> getSequenceNr();
    void setSequenceNr(const nautilus::val<uint64_t>& seqNumber);

    /// Sets the chunk number of the underlying tuple buffer. The chunk number is a monotonically increasing identifier for chunks of a sequence number.
    void setChunkNr(const nautilus::val<uint64_t>& chunkNumber);
    nautilus::val<uint64_t> getChunkNr();
    void setLastChunk(const nautilus::val<bool>& isLastChunk);
    nautilus::val<bool> isLastChunk();

    ///  Get the watermark timestamp of the underlying tuple buffer. The watermark timestamp is a point in time that guarantees no records
    ///  with a lower timestamp will be received.
    nautilus::val<uint64_t> getWatermarkTs();
    void setWatermarkTs(const nautilus::val<uint64_t>& watermarkTs);

    /// Get the creation timestamp of the underlying tuple buffer. The creation timestamp is the point in time when the tuple buffer was created.
    nautilus::val<uint64_t> getCreatingTs();
    void setCreationTs(const nautilus::val<uint64_t>& creationTs);

    ~RecordBuffer() = default;

private:
    nautilus::val<Memory::TupleBuffer*> tupleBufferRef;
};

} /// namespace NES::Nautilus
