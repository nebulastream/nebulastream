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
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{
namespace
{
const uint8_t* loadAssociatedTextValue(const Memory::TupleBuffer* tupleBuffer, const uint32_t childIndex)
{
    auto childBuffer = tupleBuffer->loadChildBuffer(childIndex);
    return childBuffer.getBuffer<uint8_t>();
}

uint32_t storeAssociatedTextValueProxy(
    const Memory::TupleBuffer* tupleBuffer,
    Memory::AbstractBufferProvider* bufferProvider,
    int8_t* textValue,
    const uint32_t size,
    const bool ownsBuffer)
{
    PRECONDITION(*std::bit_cast<uint32_t*>(textValue) == size, "VarSized size does not match the expected size.");
    auto varSizeBuffer = [&]
    {
        if (ownsBuffer)
        {
            /// If the VariableSizedData has already been allocated in an exclusive buffer we can reuse the allocation.
            return Memory::TupleBuffer::reinterpretAsTupleBuffer(textValue);
        }
        /// If the VariableSizedData does not own its buffer we cannot reuse the owning tuple buffer and thus have
        /// to create a new alloction where the VariableSizedData owns the buffer again.
        auto newBuffer = bufferProvider->getUnpooledBuffer(size + sizeof(uint32_t));
        if (!newBuffer)
        {
            throw BufferAllocationFailure("Could not allocate unpooled buffer, when storing variable sized data.");
        }
        *newBuffer.value().getBuffer<uint32_t>() = size;
        memcpy(newBuffer.value().getBuffer<int8_t>() + sizeof(uint32_t), textValue + sizeof(uint32_t), size);
        return *newBuffer;
    }();

    return tupleBuffer->storeChildBuffer(varSizeBuffer);
}
}

VarVal TupleBufferMemoryProvider::loadValue(
    const std::shared_ptr<PhysicalType>& type, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(type))
    {
        return VarVal::readVarValFromMemory(fieldReference, type);
    }
    if (NES::Util::instanceOf<VariableSizedDataPhysicalType>(type))
    {
        const auto childIndex = Nautilus::Util::readValueFromMemRef<uint32_t>(fieldReference);
        const auto textPtr = invoke(loadAssociatedTextValue, recordBuffer.getReference(), childIndex);
        return VariableSizedData(textPtr, true);
    }
    throw NotImplemented("Physical Type: type {} is currently not supported", type->toString());
}


VarVal TupleBufferMemoryProvider::storeValue(
    const std::shared_ptr<PhysicalType>& type,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    VarVal value,
    const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(type))
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = Nautilus::Util::storeValueFunctionMap.find(NES::Util::as<BasicPhysicalType>(type)->nativeType);
            storeFunction != Nautilus::Util::storeValueFunctionMap.end())
        {
            return storeFunction->second(value, fieldReference);
        }
        throw UnsupportedOperation("Physical Type: {} is currently not supported", type->toString());
    }

    if (NES::Util::instanceOf<VariableSizedDataPhysicalType>(type))
    {
        const auto textValue = value.cast<VariableSizedData>();
        const auto childIndex = nautilus::invoke(
            storeAssociatedTextValueProxy,
            recordBuffer.getReference(),
            bufferProvider,
            textValue.getReference(),
            textValue.getContentSize(),
            textValue.ownsBuffer());
        auto fieldReferenceCastedU32 = static_cast<nautilus::val<uint32_t*>>(fieldReference);
        *fieldReferenceCastedU32 = childIndex;
        return value;
    }
    throw NotImplemented("Physical Type: type {} is currently not supported", type->toString());
}


bool TupleBufferMemoryProvider::includesField(
    const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

TupleBufferMemoryProvider::~TupleBufferMemoryProvider() = default;

std::shared_ptr<TupleBufferMemoryProvider>
TupleBufferMemoryProvider::create(const uint64_t bufferSize, const std::shared_ptr<Schema>& schema)
{
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        auto rowMemoryLayout = Memory::MemoryLayouts::RowLayout::create(std::move(schema), bufferSize);
        return std::make_unique<RowTupleBufferMemoryProvider>(rowMemoryLayout);
    }
    else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        auto columnMemoryLayout = Memory::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
        return std::make_unique<ColumnTupleBufferMemoryProvider>(columnMemoryLayout);
    }
    throw NotImplemented("Currently only row and column layout are supported");
}
}
