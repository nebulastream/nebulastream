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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_RECORDBUFFER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_RECORDBUFFER_HPP_

#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <memory>
#include <ostream>
#include <vector>


namespace NES::Runtime::Execution {
using namespace nautilus;

/**
 * @brief The RecordBuffer is a representation of a set of records that are stored together.
 * In the common case this maps to a TupleBuffer, which stores the individual records in either a row or a columnar layout.
 */
class RecordBuffer {
  public:
    /**
     * @brief Creates a new record buffer with a reference to a tuple buffer
     * @param tupleBufferRef
     */
    explicit RecordBuffer(const Nautilus::MemRefVal& tupleBufferRef);

    /**
     * @brief Read number of record that are currently stored in the record buffer.
     * @return Nautilus::UInt64Val
     */
    Nautilus::UInt64Val getNumRecords();

    /**
     * @brief Retrieve the reference to the underling buffer from the record buffer.
     * @return val<void*>
     */
    [[nodiscard]] Nautilus::MemRefVal getBuffer() const;

    /**
     * @brief Get the reference to the TupleBuffer
     * @return val<void*>
     */
    [[nodiscard]] const Nautilus::MemRefVal& getReference() const;

    /**
     * @brief Set the number of records in the underlying tuple buffer.
     * @param numRecordsValue Nautilus::UInt64Val containing the number of records
     * to set in the tuple buffer.
     */
    void setNumRecords(const Nautilus::UInt64Val& numRecordsValue);

    /**
     * @brief Get the origin ID of the underlying tuple buffer.
     * @return Nautilus::UInt64Val containing the origin ID of the tuple buffer.
     */
    Nautilus::UInt64Val getOriginId();

    /**
     * @brief Get the statistic ID of the underlying tuple buffer.
     * @return Nautilus::UInt64Val containing the statistic ID of the tuple buffer.
     */
    Nautilus::UInt64Val getStatisticId();

    /**
     * @brief Set the origin ID of the underlying tuple buffer.
     * @param originId Nautilus::UInt64Val containing the origin ID to set for the
     * tuple buffer.
     */
    void setOriginId(const Nautilus::UInt64Val& originId);

    /**
     * @brief Set the origin ID of the underlying tuple buffer.
     * @param originId Nautilus::UInt64Val containing the origin ID to set for the
     * tuple buffer.
     */
    void setStatisticId(const Nautilus::UInt64Val& statisticId);

    /**
     * @brief Get the sequence number of the underlying tuple buffer.
     * The sequence number is a monotonically increasing identifier for tuple buffers from the same origin.
     * @return Nautilus::UInt64Val containing the sequence number of the tuple buffer.
     */
    Nautilus::UInt64Val getSequenceNr();

    /**
     * @brief Set the sequence number of the underlying tuple buffer.
     * @param originId Nautilus::UInt64Val containing the sequence number to set for the
     * tuple buffer.
     */
    void setSequenceNr(const Nautilus::UInt64Val& seqNumber);

    /**
     * @brief Sets the chunk number for the tuple buffer
     * @param chunkNumber
     */
    void setChunkNr(const Nautilus::UInt64Val& chunkNumber);

    /**
     * @brief Gets the chunk number of the underlying tuple buffer
     * @return Nautilus::UInt64Val
     */
    Nautilus::UInt64Val getChunkNr();

    /**
     * @brief Sets the last chunk for the tuple buffer
     * @param chunkNumber
     */
    void setLastChunk(const Nautilus::BooleanVal& isLastChunk);

    /**
     * @brief Gets if this is the last chunk for a sequence number
     * @return Nautilus::BooleanVal
     */
    Nautilus::BooleanVal isLastChunk();

    /**
     * @brief Get the watermark timestamp of the underlying tuple buffer.
     * The watermark timestamp is a point in time that guarantees no records with
     * a lower timestamp will be received.
     *
     * @return Nautilus::UInt64Val containing the watermark timestamp of the tuple buffer.
     */
    Nautilus::ExecDataType getWatermarkTs();

    /**
     * @brief Set the watermark timestamp of the underlying tuple buffer.
     * @param watermarkTs Nautilus::UInt64Val containing the watermark timestamp to set
     * for the tuple buffer.
     */
    void setWatermarkTs(const Nautilus::ExecDataType& watermarkTs);

    /**
     * @brief Get the creation timestamp of the underlying tuple buffer.
     * The creation timestamp is the point in time when the tuple buffer was
     * created.
     *
     * @return Nautilus::ExecDataType containing the creation timestamp of the tuple buffer.
     */
    Nautilus::ExecDataType getCreatingTs();

    /**
     * @brief Set the creation timestamp of the underlying tuple buffer.
     * @param creationTs Nautilus::UInt64Val containing the creation timestamp to set
     * for the tuple buffer.
     */
    void setCreationTs(const Nautilus::ExecDataType& creationTs);

    ~RecordBuffer() = default;

  private:
    Nautilus::MemRefVal tupleBufferRef;
};

}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_INCLUDE_EXECUTION_RECORDBUFFER_HPP_
