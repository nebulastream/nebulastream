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
#include <optional>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Describes a single delta computation: reads a source field and writes the delta to a target field.
struct PhysicalDeltaExpression
{
    PhysicalFunction readFunction;
    Record::RecordFieldIdentifier targetField;
    DataType targetDataType;
};

/// Describes the layout of a field for serialization to/from raw memory.
struct DeltaFieldLayoutEntry
{
    std::string fieldName;
    DataType::Type type;
};

/// Computes the difference between consecutive values across buffers.
/// The first tuple of the entire stream is dropped because there is no previous value.
/// Uses a DeltaOperatorHandler for cross-buffer state persistence via a two-way handshake.
class DeltaPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    DeltaPhysicalOperator(
        std::vector<PhysicalDeltaExpression> deltaExpressions,
        OperatorHandlerId operatorHandlerId,
        std::vector<DeltaFieldLayoutEntry> deltaFieldLayout,
        size_t deltaFieldsEntrySize,
        std::vector<DeltaFieldLayoutEntry> fullRecordLayout,
        size_t fullRecordEntrySize);

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::vector<PhysicalDeltaExpression> deltaExpressions;
    OperatorHandlerId operatorHandlerId;
    std::vector<DeltaFieldLayoutEntry> deltaFieldLayout;
    size_t deltaFieldsEntrySize;
    std::vector<DeltaFieldLayoutEntry> fullRecordLayout;
    size_t fullRecordEntrySize;
    std::optional<PhysicalOperator> child;
};

}
