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
#include <TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES
{
class LowerSchemaProvider;
}

namespace NES
{

/// Implements BufferRef. Provides columnar memory access.
class ColumnTupleBufferRef final : public TupleBufferRef
{
    struct Field
    {
        Record::RecordFieldIdentifier name;
        DataType type;
        uint64_t columnOffset;
    };

    std::vector<Field> fields;

    /// Private constructor to prevent direct instantiation
    explicit ColumnTupleBufferRef(std::vector<Field> fields, uint64_t capacity, uint64_t bufferSize);

    /// Allow LowerSchemaProvider::lowerSchema() access to private constructor and Field
    friend class NES::LowerSchemaProvider;

public:
    ColumnTupleBufferRef(const ColumnTupleBufferRef&) = default;
    ColumnTupleBufferRef(ColumnTupleBufferRef&&) = default;
    ~ColumnTupleBufferRef() override = default;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;
    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    void writeRecord(
        nautilus::val<uint64_t>& recordIndex,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;
};

}
