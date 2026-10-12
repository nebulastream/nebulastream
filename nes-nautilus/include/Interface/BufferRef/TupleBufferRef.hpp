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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/BufferRef/BufferMerge.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/RecordLayoutUtil.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{


/// This class takes care of reading and writing data from/to a TupleBuffer.
/// A TupleBufferRef is closely coupled with a memory layout, and we support row and column layouts, currently.
/// We store multiple variable sized datas in one pooled buffer. If the pooled buffer is not large enough or there are no pooled buffer
/// available, we fall back to an unpooled buffer.
class TupleBufferRef
{
protected:
    uint64_t capacity;
    uint64_t bufferSize;
    uint64_t tupleSize;

public:
    TupleBufferRef(uint64_t capacity, uint64_t bufferSize, uint64_t tupleSize);
    virtual ~TupleBufferRef();

    /// @brief Writes the variable sized data to the buffer
    static VariableSizedAccess
    writeVarSized(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, std::span<const std::byte> varSizedValue);

    /// @brief Reads the variable sized data and returns the pointer to the var sized data
    /// @return Pointer to variable sized data
    static std::span<std::byte>
    loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, VariableSizedAccess variableSizedAccess) noexcept;

    /// Reads a record from the given bufferAddress and recordIndex.
    /// @param projections: Stores what fields, the Record should contain. If {}, then Record contains all fields available
    /// @param recordBuffer: Stores the memRef to the memory segment of a tuplebuffer, e.g., tuplebuffer.getBuffer()
    /// @param recordIndex: Index of the record to be read
    virtual Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const
        = 0;

    /// Returned by writeRecord
    /// Will give information on whether the write operation was successful (record index was inbounds)
    /// and the number of records (or bytes, in case of OutputFormatterBufferRef) that were written
    struct WriteRecordResult
    {
        nautilus::val<bool> successful;
        nautilus::val<uint64_t> writtenRecords;
    };

    /// Writes a record from the given bufferAddress and recordIndex.
    /// @param recordBuffer: Stores the memRef to the memory segment of a tuplebuffer, e.g., tuplebuffer.getMemArea()
    /// @param recordIndex: Index of the record to be stored to
    /// @param rec: Record to be stored
    virtual WriteRecordResult writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
        = 0;

    /// Describes this layout for `appendTuples`, or nullopt if its buffers cannot be concatenated.
    [[nodiscard]] virtual std::optional<BufferLayout> getBufferLayout() const { return std::nullopt; }

    [[nodiscard]] uint64_t getCapacity() const;
    [[nodiscard]] uint64_t getBufferSize() const;
    [[nodiscard]] uint64_t getTupleSize() const;
    [[nodiscard]] virtual std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const = 0;
    [[nodiscard]] virtual std::vector<DataType> getAllDataTypes() const = 0;

protected:
    /// Builds the variable-sized load callback for a RecordBuffer-backed layout (row/column). Varsized
    /// payloads live in the record buffer's child buffers, so the callback closes over the buffer.
    static VarSizedLoadFn getRecordBufferLoad(const RecordBuffer& recordBuffer);

    /// Builds the variable-sized store callback for a RecordBuffer-backed layout (row/column). New varsized
    /// payloads are appended to the record buffer's child buffers via the buffer provider.
    static VarSizedStoreFn
    getRecordBufferStore(const RecordBuffer& recordBuffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider);

    /// Bytes in front of the value of a field of `type`, which hold the null flag of a nullable field.
    [[nodiscard]] static uint64_t getNullFlagSize(const DataType& type)
    {
        return type.getSizeInBytesWithNull() - type.getSizeInBytesWithoutNull();
    }

    [[nodiscard]] static bool
    includesField(const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex);
};

}
