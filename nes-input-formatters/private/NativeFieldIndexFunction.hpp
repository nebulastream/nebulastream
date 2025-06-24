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
#include <functional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <FieldIndexFunction.hpp>
#include <NativeInputFormatIndexer.hpp>

#include <InputFormatterTask.hpp>
#include "Nautilus/DataTypes/DataTypesUtil.hpp"
#include "Nautilus/Interface/Record.hpp"
#include "Nautilus/Interface/RecordBuffer.hpp"


namespace NES::Nautilus
{
class RecordBuffer;
}
namespace NES::InputFormatters
{

inline uint64_t getFieldOffset(const uint64_t fieldIndex, const std::vector<size_t>& fieldOffsets)
{
    PRECONDITION(
        fieldIndex < fieldOffsets.size(),
        "field index: {} is larger the number of field in the memory layout {}",
        fieldIndex,
        fieldOffsets.size());
    return fieldOffsets.at(fieldIndex);
}

inline nautilus::val<int8_t*>
calculateFieldAddress(const nautilus::val<int8_t*>& recordOffset, const uint64_t fieldIndex, const std::vector<size_t>& fieldOffsets)
{
    const auto fieldOffset = getFieldOffset(fieldIndex, fieldOffsets);
    auto fieldAddress = recordOffset + nautilus::val<uint64_t>(fieldOffset);
    return fieldAddress;
}

inline const uint8_t* loadAssociatedTextValue(const Memory::TupleBuffer* tupleBuffer, const uint32_t childIndex)
{
    auto childBuffer = tupleBuffer->loadChildBuffer(childIndex);
    return childBuffer.getBuffer<uint8_t>();
}

inline Nautilus::VarVal
loadValue(const DataType& physicalType, const Nautilus::RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        return Nautilus::VarVal::readVarValFromMemory(fieldReference, physicalType.type);
    }
    const auto childIndex = Nautilus::Util::readValueFromMemRef<uint32_t>(fieldReference);
    const auto textPtr = invoke(loadAssociatedTextValue, recordBuffer.getReference(), childIndex);
    return Nautilus::VariableSizedData(textPtr);
}

inline bool includesField(
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections, const Nautilus::Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

/// Determines the in-tuple-buffer representation of the offsets for type-safety reasons
template <NES::Schema::MemoryLayoutType MemoryLayoutType>
class NativeFieldIndexFunction final : public FieldIndexFunction<NativeFieldIndexFunction<MemoryLayoutType>>
{
    friend FieldIndexFunction<NativeFieldIndexFunction>;

    /// FieldIndexFunction (CRTP) interface functions
    // [[nodiscard]] FieldOffsetsType applyGetOffsetOfFirstTupleDelimiter() const { return this->offsetOfFirstTuple; }
    // [[nodiscard]] FieldOffsetsType applyGetOffsetOfLastTupleDelimiter() const { return this->offsetOfLastTuple; }
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    template <IndexerMetaDataType IndexerMetaData>
    requires std::is_same_v<IndexerMetaData, NativeMetaData>
    [[nodiscard]] Nautilus::Record applyReadNextRecord(
        const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
        const Nautilus::RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        const size_t,
        const std::vector<RawValueParser::ParseFunctionSignature>&) const
    requires(MemoryLayoutType == NES::Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        /// read all fields
        Nautilus::Record record;
        const auto tupleSize = metaData.getSchema().getSizeOfSchemaInBytes();
        const auto bufferAddress = recordBuffer.getBuffer();
        const auto recordOffset = bufferAddress + (tupleSize * recordIndex);
        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getSchema().getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getSchema().getFieldAt(i).name;
            if (not includesField(projections, fieldName))
            {
                continue;
            }
            auto fieldAddress = calculateFieldAddress(recordOffset, i, metaData.getFieldOffsets());
            auto value = loadValue(metaData.getSchema().getFieldAt(i).dataType, recordBuffer, fieldAddress);
            record.write(metaData.getSchema().getFieldAt(i).name, value);
        }
        return record;
    }

public:
    explicit NativeFieldIndexFunction() = default;
    ~NativeFieldIndexFunction() = default;
};
}
