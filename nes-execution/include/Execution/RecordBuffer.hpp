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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_RECORDBUFFER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_RECORDBUFFER_HPP_

#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Nautilus {
class Record;
}

namespace NES::Runtime::Execution {
using namespace Nautilus;

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
    explicit RecordBuffer(const Value<MemRef>& tupleBufferRef);

    /**
     * @brief Read number of record that are currently stored in the record buffer.
     * @return Value<UInt64>
     */
    Value<UInt64> getNumRecords();

    /**
     * @brief Retrieve the reference to the underling buffer from the record buffer.
     * @return Value<MemRef>
     */
    Value<MemRef> getBuffer() const;

    /**
     * @brief Get the reference to the TupleBuffer
     * @return Value<MemRef>
     */
    const Value<MemRef>& getReference() const;

    /**
     * @brief Set the number of records in the underlying tuple buffer.
     * @param numRecordsValue Value<UInt64> containing the number of records
     * to set in the tuple buffer.
     */
    void setNumRecords(const Value<UInt64>& numRecordsValue);

    /**
     * @brief Get the origin ID of the underlying tuple buffer.
     * @return Value<UInt64> containing the origin ID of the tuple buffer.
     */
    Value<UInt64> getOriginId();

    /**
     * @brief Set the origin ID of the underlying tuple buffer.
     * @param originId Value<UInt64> containing the origin ID to set for the
     * tuple buffer.
     */
    void setOriginId(const Value<UInt64>& originId);

    /**
     * @brief Get the sequence number of the underlying tuple buffer.
     * The sequence number is a monotonically increasing identifier for tuple buffers from the same origin.
     * @return Value<UInt64> containing the sequence number of the tuple buffer.
     */
    Value<UInt64> getSequenceNr();

    /**
     * @brief Set the sequence number of the underlying tuple buffer.
     * @param originId Value<UInt64> containing the sequence number to set for the
     * tuple buffer.
     */
    void setSequenceNr(const Value<UInt64>& seqNumber);

    /**
     * @brief Get the watermark timestamp of the underlying tuple buffer.
     * The watermark timestamp is a point in time that guarantees no records with
     * a lower timestamp will be received.
     *
     * @return Value<UInt64> containing the watermark timestamp of the tuple buffer.
     */
    Value<UInt64> getWatermarkTs();

    /**
     * @brief Set the watermark timestamp of the underlying tuple buffer.
     * @param watermarkTs Value<UInt64> containing the watermark timestamp to set
     * for the tuple buffer.
     */
    void setWatermarkTs(const Value<UInt64>& watermarkTs);

    /**
     * @brief Get the creation timestamp of the underlying tuple buffer.
     * The creation timestamp is the point in time when the tuple buffer was
     * created.
     *
     * @return Value<UInt64> containing the creation timestamp of the tuple buffer.
     */
    Value<UInt64> getCreatingTs();

    /**
     * @brief Set the creation timestamp of the underlying tuple buffer.
     * @param creationTs Value<UInt64> containing the creation timestamp to set
     * for the tuple buffer.
     */
    void setCreationTs(const Value<UInt64>& creationTs);

    ~RecordBuffer() = default;

  private:
    Value<MemRef> tupleBufferRef;
};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_RECORDBUFFER_HPP_
