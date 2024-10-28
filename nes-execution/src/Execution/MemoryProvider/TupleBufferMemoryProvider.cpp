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
#include <cstdint>
#include <string>
#include <API/Schema.hpp>
#include <Execution/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Execution/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val_ptr.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::Runtime::Execution::MemoryProvider
{

const uint8_t* loadAssociatedTextValue(const Memory::TupleBuffer* tupleBuffer, const uint32_t childIndex)
{
    auto childBuffer = tupleBuffer->loadChildBuffer(childIndex);
    return childBuffer.getBuffer<uint8_t>();
}

Nautilus::VarVal TupleBufferMemoryProvider::loadValue(
    const PhysicalTypePtr& type, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(type))
    {
        return Nautilus::VarVal::readVarValFromMemory(fieldReference, type);
    }
    else if (type->isVariableSizedDataType())
    {
        const auto childIndex = Nautilus::Util::readValueFromMemRef<uint32_t>(fieldReference);
        const auto textPtr = nautilus::invoke(loadAssociatedTextValue, recordBuffer.getReference(), childIndex);
        return Nautilus::VariableSizedData(textPtr);
    }
    throw NotImplemented("Physical Type: type {} is currently not supported", type->toString());
}

uint32_t storeAssociatedTextValueProxy(const Memory::TupleBuffer* tupleBuffer, const int8_t* textValue)
{
    auto textBuffer = Memory::TupleBuffer::reinterpretAsTupleBuffer(const_cast<int8_t*>(textValue));
    return tupleBuffer->storeChildBuffer(textBuffer);
}

Nautilus::VarVal TupleBufferMemoryProvider::storeValue(
    const NES::PhysicalTypePtr& type,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    Nautilus::VarVal value)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(type))
    {
        value.writeToMemory(fieldReference);
        return value;
    }
    else if (type->isVariableSizedDataType())
    {
        const auto textValue = value.cast<Nautilus::VariableSizedData>();
        const auto childIndex = nautilus::invoke(storeAssociatedTextValueProxy, recordBuffer.getReference(), textValue.getReference());
        auto fieldReferenceCastedU32 = static_cast<nautilus::val<uint32_t*>>(fieldReference);
        *fieldReferenceCastedU32 = childIndex;
        return value;
    }
    throw NotImplemented("Physical Type: type {} is currently not supported", type->toString());
}


bool TupleBufferMemoryProvider::includesField(
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections, const Nautilus::Record::RecordFieldIdentifier& fieldIndex)
{
    return std::find(projections.begin(), projections.end(), fieldIndex) != projections.end();
}

TupleBufferMemoryProvider::~TupleBufferMemoryProvider() = default;

MemoryProviderPtr TupleBufferMemoryProvider::create(const uint64_t bufferSize, const SchemaPtr schema)
{
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        auto rowMemoryLayout = Memory::MemoryLayouts::RowLayout::create(std::move(schema), bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::RowTupleBufferMemoryProvider>(rowMemoryLayout);
    }
    else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        auto columnMemoryLayout = Memory::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::ColumnTupleBufferMemoryProvider>(columnMemoryLayout);
    }
    throw NotImplemented("Currently only row and column layout are supported");
}

}
