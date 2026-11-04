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
#include <span>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// TupleBufferRef with the Apache Arrow columnar memory layout. It only produces Arrow-compatible memory and has no
/// dependency on the Arrow library.
/// Buffer layout: [validity_col0][validity_col1]...[data_col0][data_col1]..., each region is 8-byte aligned.
///  - Validity bitmaps use 1 bit per record, 1 = valid. They are only maintained for nullable fields.
///  - BOOLEAN data is bit-packed.
///  - VARSIZED columns store capacity + 1 int32 offsets. The payload lives in child buffers, a column with slot S owns
///    the child buffers S, S + numVarSizedColumns, S + 2 * numVarSizedColumns, ... Offsets span all buffers of a column
///    and a single value is never split across child buffers. A child buffer's numberOfTuples is its number of used bytes.
class ArrowBufferRef final : public TupleBufferRef
{
public:
    struct Field
    {
        static constexpr uint32_t NO_CHILD_BUFFER = UINT32_MAX;

        Record::RecordFieldIdentifier name;
        DataType type;
        uint32_t dataTypeSize; /// Bytes per record in the data region, sizeof(int32_t) for VARSIZED, 0 for bit-packed BOOLEAN
        uint64_t bitmapOffset;
        uint64_t dataOffset;
        uint32_t childBufferSlot;
    };

    ArrowBufferRef(std::vector<Field> fields, uint32_t numVarSizedColumns, uint64_t capacity, uint64_t bufferSize, uint64_t tupleSize);

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;
    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    WriteRecordResult writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    [[nodiscard]] const std::vector<Field>& getFields() const { return fields; }

    /// Appends @param value to the child buffers of the VARSIZED column stored at @param childSlot.
    static void appendVarSized(
        TupleBuffer& buffer,
        AbstractBufferProvider& bufferProvider,
        uint32_t childSlot,
        uint32_t numVarSizedColumns,
        std::span<const std::byte> value);

    /// Returns the child buffers holding the payload of the VARSIZED @param field, in offset order.
    [[nodiscard]] std::vector<TupleBuffer> loadVarSizedChildBuffers(const TupleBuffer& buffer, const Field& field) const;

private:
    std::vector<Field> fields;
    uint32_t numVarSizedColumns;
};

}
