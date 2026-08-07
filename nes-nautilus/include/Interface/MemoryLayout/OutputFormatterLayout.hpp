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
#include <Interface/MemoryLayout/MemoryLayout.hpp>
#include <Interface/Record.hpp>
#include <Interface/TaskBufferRef.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{
class LowerSchemaProvider;
}

namespace NES
{
class OutputFormatterLayout final : public MemoryLayout
{
    struct Field
    {
        Record::RecordFieldIdentifier name;
        DataType type;
    };

    std::vector<Field> fields;
    std::shared_ptr<OutputFormatter> formatter;

    explicit OutputFormatterLayout(std::vector<Field> fields, std::shared_ptr<OutputFormatter> formatter, uint64_t bufferSize);

    friend class NES::LowerSchemaProvider;

public:
    OutputFormatterLayout(const OutputFormatterLayout&) = default;
    OutputFormatterLayout(OutputFormatterLayout&&) = default;

    ~OutputFormatterLayout() override = default;

    [[nodiscard]] std::vector<Record::RecordFieldIdentifier> getAllFieldNames() const override;

    [[nodiscard]] std::vector<DataType> getAllDataTypes() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const TaskBufferRef& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    WriteRecordResult writeRecord(
        nautilus::val<uint64_t>& bytesWritten,
        const TaskBufferRef& recordBuffer,
        const Record& rec,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;
};
}
