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

#include <memory>
#include <ostream>
#include <vector>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Nautilus
{
class Record;
}

namespace NES::Runtime::Execution
{
using namespace Nautilus;

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
    explicit RecordBuffer(const nautilus::val<int8_t*>& tupleBufferRef);

    /**
     * @brief Read number of record that are currently stored in the record buffer.
     * @return nautilus::val<uint64_t>
     */
    nautilus::val<uint64_t> getNumRecords();

    /**
     * @brief Retrieve the reference to the underling buffer from the record buffer.
     * @return nautilus::val<int8_t*>
     */
    nautilus::val<int8_t*> getBuffer() const;

    /**
     * @brief Get the reference to the TupleBuffer
     * @return nautilus::val<int8_t*>
     */
    const nautilus::val<int8_t*>& getReference() const;

    /**
     * @brief Set the number of records in the underlying tuple buffer.
     * @param numRecordsValue nautilus::val<uint64_t> containing the number of records
     * to set in the tuple buffer.
     */
    void setNumRecords(const nautilus::val<uint64_t>& numRecordsValue);

    /**
     * @brief Get the origin ID of the underlying tuple buffer.
     * @return nautilus::val<uint64_t> containing the origin ID of the tuple buffer.
     */
    nautilus::val<uint64_t> getOriginId();

    /**
     * @brief Set the origin ID of the underlying tuple buffer.
     * @param originId nautilus::val<uint64_t> containing the origin ID to set for the
     * tuple buffer.
     */
    void setOriginId(const nautilus::val<uint64_t>& originId);

    /**
     * @brief Get the sequence number of the underlying tuple buffer.
     * The sequence number is a monotonically increasing identifier for tuple buffers from the same origin.
     * @return nautilus::val<uint64_t> containing the sequence number of the tuple buffer.
     */
    nautilus::val<uint64_t> getSequenceNr();

    /**
     * @brief Set the sequence number of the underlying tuple buffer.
     * @param originId nautilus::val<uint64_t> containing the sequence number to set for the
     * tuple buffer.
     */
    void setSequenceNr(const nautilus::val<uint64_t>& seqNumber);

    /**
     * @brief Sets the chunk number for the tuple buffer
     * @param chunkNumber
     */
    void setChunkNr(const nautilus::val<uint64_t>& chunkNumber);

    /**
     * @brief Gets the chunk number of the underlying tuple buffer
     * @return nautilus::val<uint64_t>
     */
    nautilus::val<uint64_t> getChunkNr();

    /**
     * @brief Sets the last chunk for the tuple buffer
     * @param chunkNumber
     */
    void setLastChunk(const nautilus::val<bool>& isLastChunk);

    /**
     * @brief Gets if this is the last chunk for a sequence number
     * @return nautilus::val<bool>
     */
    nautilus::val<bool> isLastChunk();

    /**
     * @brief Get the watermark timestamp of the underlying tuple buffer.
     * The watermark timestamp is a point in time that guarantees no records with
     * a lower timestamp will be received.
     *
     * @return nautilus::val<uint64_t> containing the watermark timestamp of the tuple buffer.
     */
    nautilus::val<uint64_t> getWatermarkTs();

    /**
     * @brief Set the watermark timestamp of the underlying tuple buffer.
     * @param watermarkTs nautilus::val<uint64_t> containing the watermark timestamp to set
     * for the tuple buffer.
     */
    void setWatermarkTs(const nautilus::val<uint64_t>& watermarkTs);

    /**
     * @brief Get the creation timestamp of the underlying tuple buffer.
     * The creation timestamp is the point in time when the tuple buffer was
     * created.
     *
     * @return nautilus::val<uint64_t> containing the creation timestamp of the tuple buffer.
     */
    nautilus::val<uint64_t> getCreatingTs();

    /**
     * @brief Set the creation timestamp of the underlying tuple buffer.
     * @param creationTs nautilus::val<uint64_t> containing the creation timestamp to set
     * for the tuple buffer.
     */
    void setCreationTs(const nautilus::val<uint64_t>& creationTs);

    ~RecordBuffer() = default;

private:
    nautilus::val<int8_t*> tupleBufferRef;
};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

} /// namespace NES::Runtime::Execution
