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

#include <Join/StreamJoinProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

namespace
{
/// Returns a typed null VarVal for the given DataType. Used in NULL-fill record construction
/// for outer join unmatched tuples.
VarVal makeNullVarVal(const DataType& dataType)
{
    const auto nullFlag = nautilus::val<bool>{true};
    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN:
            return {nautilus::val<bool>{false}, true, nullFlag};
        case DataType::Type::INT8:
            return {nautilus::val<int8_t>{0}, true, nullFlag};
        case DataType::Type::INT16:
            return {nautilus::val<int16_t>{0}, true, nullFlag};
        case DataType::Type::INT32:
            return {nautilus::val<int32_t>{0}, true, nullFlag};
        case DataType::Type::INT64:
            return {nautilus::val<int64_t>{0}, true, nullFlag};
        case DataType::Type::UINT8:
            return {nautilus::val<uint8_t>{0}, true, nullFlag};
        case DataType::Type::UINT16:
            return {nautilus::val<uint16_t>{0}, true, nullFlag};
        case DataType::Type::UINT32:
            return {nautilus::val<uint32_t>{0}, true, nullFlag};
        case DataType::Type::UINT64:
            return {nautilus::val<uint64_t>{0}, true, nullFlag};
        case DataType::Type::FLOAT32:
            return {nautilus::val<float>{0}, true, nullFlag};
        case DataType::Type::FLOAT64:
            return {nautilus::val<double>{0}, true, nullFlag};
        case DataType::Type::CHAR:
            return {nautilus::val<char>{0}, true, nullFlag};
        case DataType::Type::VARSIZED:
            return {VariableSizedData(nautilus::val<int8_t*>{nullptr}, nautilus::val<uint64_t>{0}), true, nullFlag};
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Cannot null-fill field of type {}", magic_enum::enum_name(dataType.type));
    }
    std::unreachable();
}
} /// anonymous namespace

StreamJoinProbePhysicalOperator::StreamJoinProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId, PhysicalFunction joinFunction, WindowMetaData windowMetaData, JoinSchema joinSchema)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , joinFunction(std::move(joinFunction))
    , joinSchema(std::move(joinSchema))
{
}

void StreamJoinProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.inputBufferSize = recordBuffer.getBufferSize();
    openChild(executionCtx, recordBuffer);
}

Record StreamJoinProbePhysicalOperator::createJoinedRecord(
    const Record& outerRecord,
    const Record& innerRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const std::vector<Record::RecordFieldIdentifier>& projectionsOuter,
    const std::vector<Record::RecordFieldIdentifier>& projectionsInner) const
{
    Record joinedRecord;

    /// Writing the window start, end, and key field
    joinedRecord.write(windowMetaData.startField.getFullyQualifiedName(), windowStart.convertToValue());
    joinedRecord.write(windowMetaData.endField.getFullyQualifiedName(), windowEnd.convertToValue());

    /// Writing the outerSchema fields, expect the join schema to have the fields in the same order then the outer schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsOuter))
    {
        joinedRecord.write(fieldName, outerRecord.read(fieldName));
    }

    /// Writing the innerSchema fields, expect the join schema to have the fields in the same order then the inner schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsInner))
    {
        joinedRecord.write(fieldName, innerRecord.read(fieldName));
    }

    return joinedRecord;
}

Record StreamJoinProbePhysicalOperator::createNullFilledJoinedRecord(
    const Record& preservedRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const std::vector<Record::RecordFieldIdentifier>& preservedProjections,
    const Schema<QualifiedUnboundField, Ordered>& nullSideSchema) const
{
    Record joinedRecord;

    /// Writing the window start and end fields
    joinedRecord.write(windowMetaData.startField.getFullyQualifiedName(), windowStart.convertToValue());
    joinedRecord.write(windowMetaData.endField.getFullyQualifiedName(), windowEnd.convertToValue());

    /// Writing preserved-side fields normally (same pattern as createJoinedRecord)
    for (const auto& fieldName : nautilus::static_iterable(preservedProjections))
    {
        joinedRecord.write(fieldName, preservedRecord.read(fieldName));
    }

    /// Writing null-filled fields for the null side — type must match schema DataType
    const std::vector<QualifiedUnboundField> nullSideFields(nullSideSchema.begin(), nullSideSchema.end());
    for (const auto& field : nautilus::static_iterable(nullSideFields))
    {
        auto nullVal = makeNullVarVal(field.getDataType());
        joinedRecord.write(field.getFullyQualifiedName(), nullVal);
    }

    return joinedRecord;
}
}
