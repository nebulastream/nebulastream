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
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// TupleBufferRef whose buffers hold an Apache Arrow record batch: once finalized, every column consists of standard Arrow buffers
/// that the Arrow C Data Interface and IPC can hand over without conversion. VARSIZED is Arrow `binary`.
///
/// The buffer starts with a header that gives, for each field, where its validity bitmap, its data (offsets for `binary`) and its
/// `binary` values are: in the buffer itself or in one of its child buffers, at a byte offset. A scan reads the columns through the
/// header, so a buffer can also hold a record batch that was laid out by someone else, such as the body of an IPC message.
///
/// Buffers this layout writes keep their columns behind the header, each region 8-byte aligned and sized for the capacity:
///  - Validity bitmaps, 1 bit per record and 1 = valid, exist only for nullable fields.
///  - BOOLEAN data is bit-packed.
///  - While records are written, a VARSIZED field holds a pointer and a size per record, 16 bytes. `finalize` copies the values into
///    one contiguous child buffer and replaces the pointers by the capacity + 1 int32 offsets of Arrow `binary`.
class ArrowBufferRef final : public TupleBufferRef
{
public:
    /// Where a region of a field is: `buffer` is MAIN_BUFFER for the buffer itself, the child buffer index + 1 for a child buffer,
    /// or NO_BUFFER if the region does not exist, like the bitmap of a column without NULLs.
    struct Location
    {
        static constexpr uint32_t MAIN_BUFFER = 0;
        static constexpr uint32_t NO_BUFFER = UINT32_MAX;

        uint32_t buffer;
        uint32_t offset;
    };

    enum class Region : uint8_t
    {
        VALIDITY = 0,
        DATA = 1,
        VALUES = 2,
    };

    struct Field
    {
        static constexpr uint64_t NO_BITMAP = UINT64_MAX;

        Record::RecordFieldIdentifier name;
        DataType type;
        /// Bytes per record in the data region while records are written: 16 for VARSIZED, 0 for bit-packed BOOLEAN.
        uint32_t dataTypeSize;
        /// Where this layout writes the bitmap, NO_BITMAP for a field that is not nullable.
        uint64_t bitmapOffset;
        uint64_t dataOffset;
    };

    ArrowBufferRef(std::vector<Field> fields, uint64_t headerSize, uint64_t capacity, uint64_t bufferSize, uint64_t tupleSize);

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

    void finalize(
        const RecordBuffer& recordBuffer,
        const nautilus::val<uint64_t>& numberOfRecords,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    [[nodiscard]] const std::vector<Field>& getFields() const { return fields; }

    /// Bytes at the start of a buffer that the header takes, 8-byte aligned.
    [[nodiscard]] uint64_t getHeaderSize() const { return headerSize; }

    [[nodiscard]] Location getLocation(const TupleBuffer& buffer, size_t field, Region region) const;
    void setLocation(TupleBuffer& buffer, size_t field, Region region, Location location) const;

    /// The start of a region, nullptr if the buffer has none. Child buffers stay alive as long as the buffer does.
    [[nodiscard]] static const std::byte* resolve(const TupleBuffer& buffer, Location location);

    /// Copies the VARSIZED values of the first `numberOfRecords` records into Arrow `binary` columns and writes the header.
    void finalizeBuffer(TupleBuffer& buffer, AbstractBufferProvider& bufferProvider, uint64_t numberOfRecords) const;

private:
    [[nodiscard]] uint64_t headerPosition(size_t field, Region region) const;

    std::vector<Field> fields;
    uint64_t headerSize;
};

}
