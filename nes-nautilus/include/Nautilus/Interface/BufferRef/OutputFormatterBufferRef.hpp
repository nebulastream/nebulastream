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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{
class LowerSchemaProvider;
}

namespace NES
{
class OutputFormatterBufferRef final : public TupleBufferRef
{
    struct Field
    {
        Record::RecordFieldIdentifier name;
        DataType type;
        uint64_t fieldOffset;
    };

    std::vector<Field> fields;
    std::shared_ptr<OutputFormatter> formatter;

    explicit OutputFormatterBufferRef(std::vector<Field> fields, std::shared_ptr<OutputFormatter> formatter, uint64_t tupleSize, uint64_t bufferSize);

    friend class NES::LowerSchemaProvider;

public:
    OutputFormatterBufferRef(const OutputFormatterBufferRef&) = default;
    OutputFormatterBufferRef(OutputFormatterBufferRef&&) = default;

    ~OutputFormatterBufferRef() override = default;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;

    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    nautilus::val<size_t> writeRecord(
        nautilus::val<uint64_t>& bytesWritten,
        const RecordBuffer& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        nautilus::val<bool> checkSpaceBeforeWriting = nautilus::val<bool>(false)) const override;
};
}
