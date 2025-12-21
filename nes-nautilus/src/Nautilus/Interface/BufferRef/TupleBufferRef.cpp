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

namespace
{
[[maybe_unused]]
VariableSizedData loadVarSized1(const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    thread_local struct
    {
        int8_t* ptr;
        size_t size;
    } ptrAndSize;

    nautilus::val<int8_t**> ptr(&ptrAndSize.ptr);
    nautilus::val<size_t*> size(&ptrAndSize.size);

    invoke(
        +[](TupleBuffer* tupleBuffer, int8_t* fieldReference, int8_t** ptr, size_t* size)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            const auto varSizedAccess = VariableSizedAccess::readFrom(*tupleBuffer, fieldReference);
            auto varSizedData = varSizedAccess.access(*tupleBuffer);
            *ptr = reinterpret_cast<int8_t*>(varSizedData.data());
            *size = varSizedData.size();
        },
        recordBuffer.getReference(),
        fieldReference,
        ptr,
        size);

    return VariableSizedData(*ptr, *size);
}

VariableSizedData loadVarSized2(const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    auto ptr = invoke(
        +[](TupleBuffer* tupleBuffer, int8_t* fieldReference)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            const auto varSizedAccess = VariableSizedAccess::readFrom(*tupleBuffer, fieldReference);
            auto varSizedData = varSizedAccess.access(*tupleBuffer);
            return varSizedData.data();
        },
        recordBuffer.getReference(),
        fieldReference);

    auto size = invoke(
        +[](TupleBuffer* tupleBuffer, int8_t* fieldReference)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            const auto varSizedAccess = VariableSizedAccess::readFrom(*tupleBuffer, fieldReference);
            auto varSizedData = varSizedAccess.access(*tupleBuffer);
            return varSizedData.size();
        },
        recordBuffer.getReference(),
        fieldReference);

    return VariableSizedData(ptr, size);
}

void storeVarsizedData(
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    const VariableSizedData& data,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    invoke(
        +[](TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider,
            const int8_t* varSizedPtr,
            const uint32_t varSizedValueLength,
            const int8_t* fieldReference)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            const std::span varSizedValueSpan{reinterpret_cast<const std::byte*>(varSizedPtr), varSizedValueLength};
            VariableSizedAccess::writeTo(*tupleBuffer, fieldReference, *bufferProvider, varSizedValueSpan);
        },
        recordBuffer.getReference(),
        bufferProvider,
        data.getReference(),
        data.getSize(),
        fieldReference);
}

}

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (physicalType.type == DataType::Type::VARSIZED)
    {
        return loadVarSized2(recordBuffer, fieldReference);
    }
    return VarVal::readVarValFromMemory(fieldReference, physicalType.type);
}

VarVal TupleBufferRef::storeValue(
    const DataType& physicalType,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    VarVal value,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    if (physicalType.type == DataType::Type::VARSIZED)
    {
        storeVarsizedData(recordBuffer, fieldReference, value.cast<VariableSizedData>(), bufferProvider);
    }
    else if (const auto storeFunction = storeValueFunctionMap.find(physicalType.type); storeFunction != storeValueFunctionMap.end())
    {
        return storeFunction->second(value, fieldReference);
    }
    else
    {
        throw UnknownDataType("Physical Type: {} is currently not supported", physicalType);
    }

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
