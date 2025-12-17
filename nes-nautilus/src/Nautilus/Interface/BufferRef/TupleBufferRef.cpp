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

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/VariableSizedAccessRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

struct CombinedIndexTest
{
    uint64_t index;
    uint64_t size;
};

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(fieldReference, physicalType.type);
    }

    auto combinedIndexOffset = static_cast<nautilus::val<VariableSizedAccess::IndexOffsetSize*>>(fieldReference);

    const auto varSizedPtr = invoke(
        +[](const TupleBuffer* tupleBuffer, VariableSizedAccess::IndexOffsetSize* combinedIndexOffset)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            const VariableSizedAccess access(*combinedIndexOffset);
            return MemoryLayout::loadAssociatedVarSizedValue(*tupleBuffer, access).data();
        },
        recordBuffer.getReference(),
        combinedIndexOffset);

    const nautilus::val<uint64_t> size
        = *getMemberWithOffset<uint64_t>(combinedIndexOffset, offsetof(VariableSizedAccess::IndexOffsetSize, size));
    return VariableSizedData(varSizedPtr, size);
}

VarVal TupleBufferRef::storeValue(
    const DataType& physicalType,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    VarVal value,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = storeValueFunctionMap.find(physicalType.type); storeFunction != storeValueFunctionMap.end())
        {
            return storeFunction->second(value, fieldReference);
        }
        throw UnknownDataType("Physical Type: {} is currently not supported", physicalType);
    }

    const auto varSizedValue = value.cast<VariableSizedData>();
    auto refToIndex = static_cast<nautilus::val<VariableSizedAccess::IndexOffsetSize*>>(fieldReference);

    invoke(
        +[](TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider,
            const int8_t* varSizedPtr,
            const uint32_t varSizedValueLength,
            VariableSizedAccess::IndexOffsetSize* refToIndex)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            const std::span varSizedValueSpan{varSizedPtr, varSizedPtr + varSizedValueLength};
            const VariableSizedAccess writtenAccess
                = MemoryLayout::writeVarSized(*tupleBuffer, *bufferProvider, std::as_bytes(varSizedValueSpan));
            *refToIndex = writtenAccess.getCombinedIdxOffset();
        },
        recordBuffer.getReference(),
        bufferProvider,
        varSizedValue.getReference(),
        varSizedValue.getSize(),
        refToIndex);

    return value;
}

bool TupleBufferRef::includesField(
    const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

TupleBufferRef::~TupleBufferRef() = default;

std::shared_ptr<TupleBufferRef> TupleBufferRef::create(const uint64_t bufferSize, const Schema& schema)
{
    if (schema.memoryLayoutType == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        auto rowMemoryLayout = std::make_shared<RowLayout>(bufferSize, schema);
        return std::make_shared<RowTupleBufferRef>(std::move(rowMemoryLayout));
    }
    if (schema.memoryLayoutType == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        auto columnMemoryLayout = std::make_shared<ColumnLayout>(bufferSize, schema);
        return std::make_shared<ColumnTupleBufferRef>(std::move(columnMemoryLayout));
    }
    throw NotImplemented("Currently only row and column layout are supported");
}
}
