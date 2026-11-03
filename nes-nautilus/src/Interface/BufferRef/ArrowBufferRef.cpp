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

#include <Interface/BufferRef/ArrowBufferRef.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <span>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

ArrowBufferRef::ArrowBufferRef(
    std::vector<Field> fields, const uint32_t numVarSizedColumns, const uint64_t capacity, const uint64_t bufferSize, const uint64_t tupleSize)
    : TupleBufferRef(capacity, bufferSize, tupleSize), fields(std::move(fields)), numVarSizedColumns(numVarSizedColumns)
{
}

namespace
{
nautilus::val<int8_t*> bitByteAddress(const nautilus::val<int8_t*>& bufferAddress, const uint64_t regionOffset, const nautilus::val<uint64_t>& index)
{
    return bufferAddress + (nautilus::val<uint64_t>{regionOffset} + (index / nautilus::val<uint64_t>{8}));
}

nautilus::val<bool> readBit(const nautilus::val<int8_t*>& bufferAddress, const uint64_t regionOffset, const nautilus::val<uint64_t>& index)
{
    const auto byte = readValueFromMemRef<uint8_t>(bitByteAddress(bufferAddress, regionOffset, index));
    const auto bitPos = static_cast<nautilus::val<uint8_t>>(index % nautilus::val<uint64_t>{8});
    return static_cast<nautilus::val<uint8_t>>((byte >> bitPos) & nautilus::val<uint8_t>{1}) != nautilus::val<uint8_t>{0};
}

void writeBit(
    const nautilus::val<int8_t*>& bufferAddress, const uint64_t regionOffset, const nautilus::val<uint64_t>& index, const nautilus::val<bool>& bit)
{
    const auto byteAddress = bitByteAddress(bufferAddress, regionOffset, index);
    const auto byte = readValueFromMemRef<uint8_t>(byteAddress);
    const auto bitPos = static_cast<nautilus::val<uint8_t>>(index % nautilus::val<uint64_t>{8});
    const auto mask = static_cast<nautilus::val<uint8_t>>(nautilus::val<uint8_t>{1} << bitPos);
    const auto withBitSet = static_cast<nautilus::val<uint8_t>>(byte | mask);
    const auto withBitCleared = static_cast<nautilus::val<uint8_t>>(byte & static_cast<nautilus::val<uint8_t>>(mask ^ nautilus::val<uint8_t>{0xFF}));
    VarVal{nautilus::select(bit, withBitSet, withBitCleared)}.writeToMemory(byteAddress);
}

const std::byte* findVarSized(const TupleBuffer* buffer, const uint32_t childSlot, const uint32_t numVarSizedColumns, const int32_t start)
{
    INVARIANT(buffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
    /// ponytail: linear in the number of child buffers per value, cache the current child buffer if this shows up in a profile
    uint64_t firstOffsetOfChild = 0;
    for (uint32_t index = childSlot; index < buffer->getNumberOfChildBuffers(); index += numVarSizedColumns)
    {
        const auto child = buffer->loadChildBuffer(ChildBufferIndex{index});
        const auto usedBytes = child.getNumberOfTuples();
        if (static_cast<uint64_t>(start) < firstOffsetOfChild + usedBytes)
        {
            /// The parent keeps the child alive, thus the pointer stays valid after child goes out of scope.
            return child.getAvailableMemoryArea().subspan(static_cast<uint64_t>(start) - firstOffsetOfChild).data();
        }
        firstOffsetOfChild += usedBytes;
    }
    INVARIANT(false, "Arrow offset {} exceeds the {} bytes stored for the column", start, firstOffsetOfChild);
    return nullptr;
}
}

void ArrowBufferRef::appendVarSized(
    TupleBuffer& buffer,
    AbstractBufferProvider& bufferProvider,
    const uint32_t childSlot,
    const uint32_t numVarSizedColumns,
    const std::span<const std::byte> value)
{
    const auto allocate = [&bufferProvider](const uint64_t minimumSize)
    {
        if (minimumSize <= bufferProvider.getBufferSize())
        {
            auto newBuffer = bufferProvider.getBufferBlocking();
            newBuffer.setNumberOfTuples(0);
            return newBuffer;
        }
        auto unpooled = bufferProvider.getUnpooledBuffer(minimumSize);
        if (not unpooled.has_value())
        {
            throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", minimumSize);
        }
        unpooled->setNumberOfTuples(0);
        return *unpooled;
    };
    /// Pads the child buffers with empty ones until the next stored child buffer has @param index.
    const auto padUpTo = [&](const uint32_t index)
    {
        while (buffer.getNumberOfChildBuffers() < index)
        {
            auto padding = allocate(0);
            [[maybe_unused]] const auto paddingIndex = buffer.storeChildBuffer(padding);
        }
    };

    uint32_t lastChild = childSlot;
    if (buffer.getNumberOfChildBuffers() <= childSlot)
    {
        padUpTo(childSlot);
        auto first = allocate(value.size());
        [[maybe_unused]] const auto firstIndex = buffer.storeChildBuffer(first);
    }
    else
    {
        lastChild += (buffer.getNumberOfChildBuffers() - 1 - childSlot) / numVarSizedColumns * numVarSizedColumns;
    }

    auto child = buffer.loadChildBuffer(ChildBufferIndex{lastChild});
    if (child.getNumberOfTuples() + value.size() > child.getBufferSize())
    {
        lastChild += numVarSizedColumns;
        padUpTo(lastChild);
        auto next = allocate(value.size());
        [[maybe_unused]] const auto nextIndex = buffer.storeChildBuffer(next);
        child = buffer.loadChildBuffer(ChildBufferIndex{lastChild});
    }
    const auto used = child.getNumberOfTuples();
    std::ranges::copy(value, child.getAvailableMemoryArea().subspan(used).begin());
    child.setNumberOfTuples(used + value.size());
}

Record ArrowBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    Record record;
    const auto bufferAddress = recordBuffer.getMemArea();
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& field = fields.at(i);
        if (not includesField(projections, field.name))
        {
            continue;
        }

        nautilus::val<bool> isNull{false};
        if (field.type.nullable)
        {
            isNull = !readBit(bufferAddress, field.bitmapOffset, recordIndex);
        }

        if (field.type.type == DataType::Type::BOOLEAN)
        {
            record.write(field.name, VarVal{readBit(bufferAddress, field.dataOffset, recordIndex), field.type.nullable, isNull});
            continue;
        }

        const nautilus::val<int8_t*> slot = bufferAddress + ((recordIndex * field.dataTypeSize) + field.dataOffset);
        if (field.type.type == DataType::Type::VARSIZED)
        {
            const auto start = readValueFromMemRef<int32_t>(slot);
            const auto end = readValueFromMemRef<int32_t>(slot + nautilus::val<uint64_t>{sizeof(int32_t)});
            const auto length = static_cast<nautilus::val<uint64_t>>(end - start);
            /// Empty values have no payload and may start at the end of the column, so they must not be looked up.
            const auto payload = invoke(
                {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                +[](const TupleBuffer* buffer, const uint32_t childSlot, const uint32_t numVarSizedColumns, const int32_t startOffset, const uint64_t size)
                { return size == 0 ? nullptr : findVarSized(buffer, childSlot, numVarSizedColumns, startOffset); },
                recordBuffer.getReference(),
                nautilus::val<uint32_t>{field.childBufferSlot},
                nautilus::val<uint32_t>{numVarSizedColumns},
                start,
                length);
            record.write(field.name, VarVal{VariableSizedData{static_cast<nautilus::val<int8_t*>>(payload), length}, field.type.nullable, isNull});
            continue;
        }
        record.write(field.name, VarVal::readVarValFromMemory(slot, field.type, isNull));
    }
    return record;
}

TupleBufferRef::WriteRecordResult ArrowBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<bool> successful{false};
    nautilus::val<uint64_t> writtenRecords{0};
    if (recordIndex < capacity)
    {
        const auto bufferAddress = recordBuffer.getMemArea();
        for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
        {
            const auto& field = fields.at(i);
            if (not rec.hasField(field.name))
            {
                continue;
            }
            const auto& value = rec.read(field.name);

            if (field.type.nullable)
            {
                writeBit(bufferAddress, field.bitmapOffset, recordIndex, !value.isNull());
            }

            if (field.type.type == DataType::Type::BOOLEAN)
            {
                writeBit(bufferAddress, field.dataOffset, recordIndex, value.getRawValueAs<nautilus::val<bool>>());
                continue;
            }

            const nautilus::val<int8_t*> slot = bufferAddress + ((recordIndex * field.dataTypeSize) + field.dataOffset);
            if (field.type.type == DataType::Type::VARSIZED)
            {
                const auto varSized = value.getRawValueAs<VariableSizedData>();
                invoke(
                    +[](TupleBuffer* buffer,
                        AbstractBufferProvider* provider,
                        int32_t* offsets,
                        const uint64_t index,
                        const std::byte* payload,
                        const uint64_t size,
                        const uint32_t childSlot,
                        const uint32_t numVarSizedColumns,
                        const bool isNull)
                    {
                        if (index == 0)
                        {
                            offsets[0] = 0;
                        }
                        /// Null values have no payload but still need their offset entry
                        const auto length = isNull ? uint64_t{0} : size;
                        offsets[1] = offsets[0] + static_cast<int32_t>(length);
                        if (length > 0)
                        {
                            appendVarSized(*buffer, *provider, childSlot, numVarSizedColumns, {payload, length});
                        }
                    },
                    recordBuffer.getReference(),
                    bufferProvider,
                    static_cast<nautilus::val<int32_t*>>(slot),
                    recordIndex,
                    static_cast<nautilus::val<std::byte*>>(varSized.getContent()),
                    varSized.getSize(),
                    nautilus::val<uint32_t>{field.childBufferSlot},
                    nautilus::val<uint32_t>{numVarSizedColumns},
                    value.isNull());
                continue;
            }
            /// The null state already lives in the validity bitmap, so the value is stored without the leading null byte.
            storeValueFunctionMap.at(field.type.type)(value, slot);
        }
        writtenRecords = 1;
        successful = true;
    }
    return {.successful = successful, .writtenRecords = writtenRecords};
}

std::vector<Record::RecordFieldIdentifier> ArrowBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform(&Field::name) | std::ranges::to<std::vector>();
}

std::vector<DataType> ArrowBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform(&Field::type) | std::ranges::to<std::vector>();
}

std::vector<TupleBuffer> ArrowBufferRef::loadVarSizedChildBuffers(const TupleBuffer& buffer, const Field& field) const
{
    PRECONDITION(field.childBufferSlot != Field::NO_CHILD_BUFFER, "Field {} is not VARSIZED", field.name);
    std::vector<TupleBuffer> children;
    for (uint32_t index = field.childBufferSlot; index < buffer.getNumberOfChildBuffers(); index += numVarSizedColumns)
    {
        children.push_back(buffer.loadChildBuffer(ChildBufferIndex{index}));
    }
    return children;
}

}
