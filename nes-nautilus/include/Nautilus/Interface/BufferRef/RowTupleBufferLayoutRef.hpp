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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/BufferLayoutRef.hpp>
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

/// @brief Implements BufferLayout with row-wise (record-at-a-time) memory access.
/// Records are stored contiguously: [record0_field0, record0_field1, ..., record1_field0, ...]
/// There is no header in the default row layout (headerSize == 0).
class RowTupleBufferLayoutRef final : public BufferLayoutRef
{
    struct Field
    {
        Record::RecordFieldIdentifier name;
        DataType type;
        uint64_t fieldOffset; ///< Byte offset of this field within a single row
    };

    std::vector<Field> fields;
    uint64_t tupleSize;
    uint64_t bufferSize;
    uint64_t capacity;

    /// Private constructor — use LowerSchemaProvider::lowerSchema() to instantiate
    explicit RowTupleBufferLayoutRef(std::vector<Field> fields, uint64_t tupleSize, uint64_t bufferSize);

    friend class NES::LowerSchemaProvider;

public:
    RowTupleBufferLayoutRef(const RowTupleBufferLayoutRef&) = default;
    RowTupleBufferLayoutRef(RowTupleBufferLayoutRef&&) = default;
    ~RowTupleBufferLayoutRef() override = default;

    [[nodiscard]] nautilus::val<int8_t*> getHeaderStart(const nautilus::val<int8_t*>& bufferBase) const override;
    [[nodiscard]] nautilus::val<int8_t*> getDataStart(const nautilus::val<int8_t*>& bufferBase) const override;
    [[nodiscard]] uint64_t getHeaderSize() const override;
    [[nodiscard]] uint64_t getCapacity() const override;
    [[nodiscard]] uint64_t getBufferSize() const override;
    [[nodiscard]] uint64_t getTupleSize() const;
    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;
    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override;

    [[nodiscard]] Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    WriteRecordResult writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;
};

} // namespace NES