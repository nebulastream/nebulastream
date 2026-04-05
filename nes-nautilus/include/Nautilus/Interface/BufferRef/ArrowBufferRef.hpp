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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{
class LowerSchemaProvider;
}

namespace NES
{

/// Implements TupleBufferRef with Apache Arrow columnar memory layout.
/// Buffer layout: [null_bitmap_col0][null_bitmap_col1]...[data_col0][data_col1]...
/// Each region is 8-byte aligned. Null bitmaps use 1 bit per record (Arrow validity bitmap format).
/// This class has no dependency on Apache Arrow headers — it only produces Arrow-compatible memory layout.
class ArrowBufferRef final : public TupleBufferRef
{
    struct Field
    {
        Record::RecordFieldIdentifier name;
        DataType type;
        uint32_t dataTypeSize;       /// Size per record in the data region (sizeof(int32_t) for VARSIZED offsets, raw type size otherwise)
        uint64_t bitmapOffset;       /// Offset to the null bitmap for this column
        uint64_t dataOffset;         /// Offset to the data region for this column
        uint32_t childBufferSlot;    /// For VARSIZED: which child buffer stores contiguous string data. UINT32_MAX for fixed-width.
        static constexpr uint32_t NO_CHILD_BUFFER = UINT32_MAX;
    };

    std::vector<Field> fields;
    uint32_t numVarSizedColumns; /// Number of VARSIZED columns (for child buffer stride in wrapBufferAsRecordBatch)

    explicit ArrowBufferRef(std::vector<Field> fields, uint32_t numVarSizedColumns, uint64_t capacity, uint64_t bufferSize, uint64_t tupleSize);

    friend class NES::LowerSchemaProvider;

public:
    /// Round up to the next multiple of 8 (Arrow alignment)
    static constexpr uint64_t align8(uint64_t value) { return (value + 7) & ~uint64_t{7}; }

    ArrowBufferRef(const ArrowBufferRef&) = default;
    ArrowBufferRef(ArrowBufferRef&&) = default;
    ~ArrowBufferRef() override = default;

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

    /// Returns the per-field metadata needed by downstream consumers (e.g. ArrowFlightSink)
    /// to zero-copy wrap buffer memory as Arrow arrays.
    [[nodiscard]] const std::vector<Field>& getFields() const { return fields; }

    /// Returns the number of VARSIZED columns (child buffer interleaving stride).
    [[nodiscard]] uint32_t getNumVarSizedColumns() const { return numVarSizedColumns; }

    /// Collect child buffers belonging to a VARSIZED column, respecting the interleaving stride.
    /// Returns them in order (first child buffer first). Sinks use this to read VARSIZED data
    /// without needing to know the stride convention.
    [[nodiscard]] std::vector<TupleBuffer> loadVarSizedColumnChildBuffers(
        const TupleBuffer& buffer,
        size_t fieldIndex) const;
};

}
