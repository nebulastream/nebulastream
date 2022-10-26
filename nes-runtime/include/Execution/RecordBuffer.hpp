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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_

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
    explicit RecordBuffer(Value<MemRef> tupleBufferRef);

    /**
     * @brief Read number of record that are currently stored in the record buffer.
     * @return Value<UInt64>
     */
    Value<UInt64> getNumRecords();

    /**
     * @brief Retrieve the reference to the underling buffer from the record buffer.
     * @return Value<MemRef>
     */
    Value<MemRef> getBuffer();

    /**
     * @brief Read a single record from a record buffer
     * @note This method should be factored out in #3092
     * @param memoryLayout memory layout
     * @param projections set of projections
     * @param bufferAddress address to the tuple buffer
     * @param recordIndex record index to read
     * @return Record
     */
    Record read(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                const std::vector<Record::RecordFieldIdentifier>& projections,
                Value<MemRef> bufferAddress,
                Value<UInt64> recordIndex);
    /**
     * @brief Write a single record from a record buffer
     * @note This method should be factored out in #3092
     * @param memoryLayout memory layout
     * @param bufferAddress address to the tuple buffer
     * @param Record record
     */
    void write(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<UInt64> recordIndex, Record& record);

    /**
     * @brief Get the reference to the TupleBuffer
     * @return Value<MemRef>
     */
    const Value<MemRef>& getReference();
    ~RecordBuffer() = default;
  public:
    Value<MemRef> tupleBufferRef;
    void setNumRecords(Value<UInt64> value);

  private:
    Record readRowLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                         const std::vector<Record::RecordFieldIdentifier>& projections,
                         Value<MemRef> bufferAddress,
                         Value<UInt64> recordIndex);
    Record readColumnarLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                              const std::vector<Record::RecordFieldIdentifier>& projections,
                              Value<MemRef> bufferAddress,
                              Value<UInt64> recordIndex);

    void writeRowLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<UInt64> recordIndex, Record& record);
    void writeColumnLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<UInt64> recordIndex, Record& record);

    bool includeField(const std::vector<Record::RecordFieldIdentifier>& projections, Record::RecordFieldIdentifier fieldIndex);
};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

}// namespace NES::Runtime::Execution

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
