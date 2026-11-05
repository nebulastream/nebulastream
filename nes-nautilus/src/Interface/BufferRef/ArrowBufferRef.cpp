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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
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
    std::vector<Field> fields, const uint64_t headerSize, const uint64_t capacity, const uint64_t bufferSize, const uint64_t tupleSize)
    : TupleBufferRef(capacity, bufferSize, tupleSize), fields(std::move(fields)), headerSize(headerSize)
{
}

namespace
{
/// A VARSIZED value as the data region holds it until `finalize`.
struct PendingValue
{
    const std::byte* data;
    uint64_t size;
};

static_assert(sizeof(PendingValue) == 16);

nautilus::val<bool> readBit(const nautilus::val<int8_t*>& region, const nautilus::val<uint64_t>& index)
{
    const auto byte = readValueFromMemRef<uint8_t>(region + (index / nautilus::val<uint64_t>{8}));
    const auto bitPos = static_cast<nautilus::val<uint8_t>>(index % nautilus::val<uint64_t>{8});
    return static_cast<nautilus::val<uint8_t>>((byte >> bitPos) & nautilus::val<uint8_t>{1}) != nautilus::val<uint8_t>{0};
}

void writeBit(
    const nautilus::val<int8_t*>& bufferAddress,
    const uint64_t regionOffset,
    const nautilus::val<uint64_t>& index,
    const nautilus::val<bool>& bit)
{
    const auto byteAddress = bufferAddress + (nautilus::val<uint64_t>{regionOffset} + (index / nautilus::val<uint64_t>{8}));
    const auto byte = readValueFromMemRef<uint8_t>(byteAddress);
    const auto bitPos = static_cast<nautilus::val<uint8_t>>(index % nautilus::val<uint64_t>{8});
    const auto mask = static_cast<nautilus::val<uint8_t>>(nautilus::val<uint8_t>{1} << bitPos);
    const auto withBitSet = static_cast<nautilus::val<uint8_t>>(byte | mask);
    const auto withBitCleared
        = static_cast<nautilus::val<uint8_t>>(byte & static_cast<nautilus::val<uint8_t>>(mask ^ nautilus::val<uint8_t>{0xFF}));
    VarVal{nautilus::select(bit, withBitSet, withBitCleared)}.writeToMemory(byteAddress);
}

TupleBuffer allocateValues(AbstractBufferProvider& bufferProvider, const uint64_t size)
{
    if (size <= bufferProvider.getBufferSize())
    {
        return bufferProvider.getBufferBlocking();
    }
    auto unpooled = bufferProvider.getUnpooledBuffer(size);
    if (!unpooled.has_value())
    {
        throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", size);
    }
    return *unpooled;
}
}

uint64_t ArrowBufferRef::headerPosition(const size_t field, const Region region) const
{
    PRECONDITION(field < fields.size(), "Field {} is out of range of {} fields", field, fields.size());
    return ((field * 3) + static_cast<uint64_t>(region)) * sizeof(Location);
}

ArrowBufferRef::Location ArrowBufferRef::getLocation(const TupleBuffer& buffer, const size_t field, const Region region) const
{
    Location location{};
    std::memcpy(&location, buffer.getAvailableMemoryArea().subspan(headerPosition(field, region)).data(), sizeof(Location));
    return location;
}

void ArrowBufferRef::setLocation(TupleBuffer& buffer, const size_t field, const Region region, const Location location) const
{
    std::memcpy(buffer.getAvailableMemoryArea().subspan(headerPosition(field, region)).data(), &location, sizeof(Location));
}

const std::byte* ArrowBufferRef::resolve(const TupleBuffer& buffer, const Location location)
{
    if (location.buffer == Location::NO_BUFFER)
    {
        return nullptr;
    }
    if (location.buffer == Location::MAIN_BUFFER)
    {
        return buffer.getAvailableMemoryArea().subspan(location.offset).data();
    }
    INVARIANT(
        location.buffer - 1 < buffer.getNumberOfChildBuffers(),
        "Arrow region in child buffer {}, but the buffer has {}",
        location.buffer - 1,
        buffer.getNumberOfChildBuffers());
    /// The parent keeps the child alive, thus the pointer stays valid after child goes out of scope.
    const auto child = buffer.loadChildBuffer(ChildBufferIndex{location.buffer - 1});
    return child.getAvailableMemoryArea().subspan(location.offset).data();
}

void ArrowBufferRef::finalizeBuffer(TupleBuffer& buffer, AbstractBufferProvider& bufferProvider, const uint64_t numberOfRecords) const
{
    constexpr Location none{.buffer = Location::NO_BUFFER, .offset = 0};
    for (size_t index = 0; index < fields.size(); ++index)
    {
        const auto& field = fields[index];
        setLocation(
            buffer,
            index,
            Region::VALIDITY,
            field.bitmapOffset == Field::NO_BITMAP
                ? none
                : Location{.buffer = Location::MAIN_BUFFER, .offset = static_cast<uint32_t>(field.bitmapOffset)});
        setLocation(buffer, index, Region::DATA, {.buffer = Location::MAIN_BUFFER, .offset = static_cast<uint32_t>(field.dataOffset)});
        setLocation(buffer, index, Region::VALUES, none);
        if (field.type.type != DataType::Type::VARSIZED)
        {
            continue;
        }

        const auto data = buffer.getAvailableMemoryArea().subspan(field.dataOffset);
        std::vector<PendingValue> pending(numberOfRecords);
        std::memcpy(pending.data(), data.data(), numberOfRecords * sizeof(PendingValue));

        uint64_t total = 0;
        for (const auto& value : pending)
        {
            total += value.size;
        }
        if (total > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()))
        {
            throw CannotAllocateBuffer("The {} bytes of VARSIZED field {} exceed the int32 offsets of Arrow binary", total, field.name);
        }

        /// The offsets take the first 4 * (numberOfRecords + 1) bytes of the 16 * capacity the pointers took, which were copied above.
        std::vector<int32_t> offsets{0};
        offsets.reserve(numberOfRecords + 1);
        std::optional<TupleBuffer> values;
        if (total > 0)
        {
            values = allocateValues(bufferProvider, total);
        }
        for (const auto& value : pending)
        {
            if (value.size > 0)
            {
                std::memcpy(values->getAvailableMemoryArea().subspan(static_cast<size_t>(offsets.back())).data(), value.data, value.size);
            }
            offsets.push_back(offsets.back() + static_cast<int32_t>(value.size));
        }
        std::memcpy(data.data(), offsets.data(), offsets.size() * sizeof(int32_t));
        if (values.has_value())
        {
            values->setNumberOfTuples(total);
            const auto childIndex = buffer.storeChildBuffer(*values);
            setLocation(buffer, index, Region::VALUES, {.buffer = childIndex.getRawValue() + 1, .offset = 0});
        }
    }
}

Record ArrowBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    Record record;
    const auto bufferAddress = recordBuffer.getMemArea();
    /// The start of the region at a location read from the header; a region in the buffer itself needs no call.
    const auto locate = [&](const nautilus::val<uint32_t>& buffer, const nautilus::val<uint32_t>& offset)
    {
        nautilus::val<int8_t*> address = bufferAddress + static_cast<nautilus::val<uint64_t>>(offset);
        if (buffer != nautilus::val<uint32_t>{Location::MAIN_BUFFER})
        {
            address = static_cast<nautilus::val<int8_t*>>(invoke(
                {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                +[](const TupleBuffer* tupleBuffer, const uint32_t bufferId, const uint32_t regionOffset)
                { return ArrowBufferRef::resolve(*tupleBuffer, {.buffer = bufferId, .offset = regionOffset}); },
                recordBuffer.getReference(),
                buffer,
                offset));
        }
        return address;
    };
    const auto readLocation = [&](const size_t field, const Region region)
    {
        const auto position = bufferAddress + nautilus::val<uint64_t>{headerPosition(field, region)};
        return std::pair{
            readValueFromMemRef<uint32_t>(position), readValueFromMemRef<uint32_t>(position + nautilus::val<uint64_t>{sizeof(uint32_t)})};
    };

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
            const auto [buffer, offset] = readLocation(i, Region::VALIDITY);
            if (buffer != nautilus::val<uint32_t>{Location::NO_BUFFER})
            {
                isNull = !readBit(locate(buffer, offset), recordIndex);
            }
        }

        const auto [dataBuffer, dataOffset] = readLocation(i, Region::DATA);
        const auto data = locate(dataBuffer, dataOffset);
        if (field.type.type == DataType::Type::BOOLEAN)
        {
            record.write(field.name, VarVal{readBit(data, recordIndex), field.type.nullable, isNull});
            continue;
        }

        if (field.type.type == DataType::Type::VARSIZED)
        {
            const auto slot = data + (recordIndex * nautilus::val<uint64_t>{sizeof(int32_t)});
            const auto start = readValueFromMemRef<int32_t>(slot);
            const auto end = readValueFromMemRef<int32_t>(slot + nautilus::val<uint64_t>{sizeof(int32_t)});
            const auto length = static_cast<nautilus::val<uint64_t>>(end - start);
            /// An empty column may have no values buffer at all.
            nautilus::val<int8_t*> payload{nullptr};
            if (length > nautilus::val<uint64_t>{0})
            {
                const auto [valuesBuffer, valuesOffset] = readLocation(i, Region::VALUES);
                payload = locate(valuesBuffer, valuesOffset) + static_cast<nautilus::val<uint64_t>>(start);
            }
            record.write(field.name, VarVal{VariableSizedData{payload, length}, field.type.nullable, isNull});
            continue;
        }
        const auto slot = data + (recordIndex * nautilus::val<uint64_t>{field.type.getSizeInBytesWithoutNull()});
        record.write(field.name, VarVal::readVarValFromMemory(slot, field.type, isNull));
    }
    return record;
}

TupleBufferRef::WriteRecordResult ArrowBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>&) const
{
    nautilus::val<bool> successful{false};
    nautilus::val<uint64_t> writtenRecords{0};
    if (recordIndex < capacity)
    {
        const auto bufferAddress = recordBuffer.getMemArea();
        for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
        {
            const auto& field = fields.at(i);
            const nautilus::val<int8_t*> slot = bufferAddress + ((recordIndex * field.dataTypeSize) + field.dataOffset);
            /// A field the record does not have, like the result of an out-of-process UDF, is NULL until someone fills it in.
            /// Its VARSIZED slot still needs a size, as `finalize` reads every slot.
            if (not rec.hasField(field.name))
            {
                if (field.bitmapOffset != Field::NO_BITMAP)
                {
                    writeBit(bufferAddress, field.bitmapOffset, recordIndex, nautilus::val<bool>{false});
                }
                if (field.type.type == DataType::Type::VARSIZED)
                {
                    invoke(
                        +[](int8_t* pendingSlot)
                        {
                            constexpr PendingValue empty{.data = nullptr, .size = 0};
                            std::memcpy(pendingSlot, &empty, sizeof(PendingValue));
                        },
                        slot);
                }
                continue;
            }
            const auto& value = rec.read(field.name);

            if (field.bitmapOffset != Field::NO_BITMAP)
            {
                writeBit(bufferAddress, field.bitmapOffset, recordIndex, !value.isNull());
            }

            if (field.type.type == DataType::Type::BOOLEAN)
            {
                writeBit(bufferAddress, field.dataOffset, recordIndex, value.getRawValueAs<nautilus::val<bool>>());
                continue;
            }

            if (field.type.type == DataType::Type::VARSIZED)
            {
                /// Only the pointer is kept: the value stays valid until the buffer is finalized, which copies it.
                const auto varSized = value.getRawValueAs<VariableSizedData>();
                invoke(
                    +[](int8_t* pendingSlot, const std::byte* content, const uint64_t size, const bool isNull)
                    {
                        const PendingValue pending{.data = isNull ? nullptr : content, .size = isNull ? 0 : size};
                        std::memcpy(pendingSlot, &pending, sizeof(PendingValue));
                    },
                    slot,
                    static_cast<nautilus::val<std::byte*>>(varSized.getContent()),
                    varSized.getSize(),
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

void ArrowBufferRef::finalize(
    const RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numberOfRecords,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    invoke(
        +[](const ArrowBufferRef* layout, TupleBuffer* buffer, AbstractBufferProvider* provider, const uint64_t records)
        { layout->finalizeBuffer(*buffer, *provider, records); },
        nautilus::val<const ArrowBufferRef*>{this},
        recordBuffer.getReference(),
        bufferProvider,
        numberOfRecords);
}

std::vector<Record::RecordFieldIdentifier> ArrowBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform(&Field::name) | std::ranges::to<std::vector>();
}

std::vector<DataType> ArrowBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform(&Field::type) | std::ranges::to<std::vector>();
}

}
