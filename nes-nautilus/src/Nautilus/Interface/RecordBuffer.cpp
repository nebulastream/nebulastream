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
#include <Nautilus/Interface/RecordBuffer.hpp>

#include <cstdint>

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Nautilus/Interface/TupleBufferProxyFunctions.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>
#include "Nautilus/DataTypes/DataTypesUtil.hpp"

namespace NES
{

RecordBuffer::RecordBuffer(const nautilus::val<TupleBuffer*>& tupleBufferRef) : tupleBufferRef(tupleBufferRef)
{
}

nautilus::val<uint64_t> RecordBuffer::getNumRecords() const
{
    return invoke(ProxyFunctions::NES_Memory_TupleBuffer_getNumberOfTuples, tupleBufferRef);
}

void RecordBuffer::setNumRecords(const nautilus::val<uint64_t>& numRecordsValue)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setNumberOfTuples, tupleBufferRef, numRecordsValue);
}

nautilus::val<int8_t*> RecordBuffer::getMemArea() const
{
    return invoke(ProxyFunctions::NES_Memory_TupleBuffer_getMemArea, tupleBufferRef);
}

const nautilus::val<TupleBuffer*>& RecordBuffer::getReference() const
{
    return tupleBufferRef;
}

nautilus::val<OriginId> RecordBuffer::getOriginId()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getOriginId, tupleBufferRef)};
}

void RecordBuffer::setOriginId(const nautilus::val<OriginId>& originId)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setOriginId, tupleBufferRef, originId);
}

RecordBuffer::SequenceRangeRef::SequenceRangeRef(
    const nautilus::val<uint64_t*>& seq_start_data, const nautilus::val<uint64_t*>& seq_end_data, const nautilus::val<size_t>& size)
    : seqStartData(seq_start_data), seqEndData(seq_end_data), size(size)
{
}

struct SequenceRangeBuffer
{
    uint64_t* start;
    uint64_t* end;
    size_t size;
};

void storeBuffer(SequenceRange* allocated, SequenceRangeBuffer* buffer)
{
    PRECONDITION(reinterpret_cast<uintptr_t>(allocated) > 0x1000,
        "storeBuffer called with invalid SequenceRange pointer: {}", reinterpret_cast<uintptr_t>(allocated));
    buffer->start = allocated->start.data();
    buffer->end = allocated->end.data();
    buffer->size = allocated->end.depth();
}

RecordBuffer::SequenceRangeRef RecordBuffer::SequenceRangeRef::from(nautilus::val<SequenceRange*> allocated)
{
    nautilus::val<SequenceRangeBuffer> buffer;
    nautilus::invoke(storeBuffer, allocated, &buffer);
    return {buffer.get(&SequenceRangeBuffer::start), buffer.get(&SequenceRangeBuffer::end), buffer.get(&SequenceRangeBuffer::size)};
}

void RecordBuffer::SequenceRangeRef::store(nautilus::val<SequenceRange*> allocated)
{
    struct Buffer
    {
        uint64_t* start;
        uint64_t* end;
        size_t size;
    };

    nautilus::val<Buffer> buffer;
    buffer.set(&Buffer::start, this->seqStartData), buffer.set(&Buffer::end, this->seqEndData), buffer.set(&Buffer::size, this->size);
    nautilus::invoke(
        +[](SequenceRange* allocated, Buffer* buffer)
        {
            allocated->start = FracturedNumber{std::span<uint64_t>(buffer->start, buffer->size) | std::ranges::to<std::vector>()};
            allocated->end = FracturedNumber{std::span<uint64_t>(buffer->end, buffer->size) | std::ranges::to<std::vector>()};
        },
        allocated,
        &buffer);
}

void throwOutOfBoundsException(size_t index, size_t size)
{
    throw std::runtime_error(fmt::format("Index out of bounds {} >= {}", index, size));
}

void RecordBuffer::SequenceRangeRef::setStart(const nautilus::val<size_t>& index, const nautilus::val<size_t>& value)
{
    if (index >= size)
    {
        nautilus::invoke(throwOutOfBoundsException, index, size);
    }
    *(seqStartData + index) = value;
}

void RecordBuffer::SequenceRangeRef::setEnd(nautilus::val<size_t> index, nautilus::val<size_t> value)
{
    if (index >= size)
    {
        nautilus::invoke(throwOutOfBoundsException, index, size);
    }
    *(seqEndData + index) = value;
}

nautilus::val<uint64_t> RecordBuffer::SequenceRangeRef::getStart(nautilus::val<size_t> index) const
{
    if (index >= size)
    {
        nautilus::invoke(throwOutOfBoundsException, index, size);
    }
    return *(seqStartData + index);
}

nautilus::val<uint64_t> RecordBuffer::SequenceRangeRef::getEnd(nautilus::val<size_t> index) const
{
    if (index >= size)
    {
        nautilus::invoke(throwOutOfBoundsException, index, size);
    }
    return *(seqEndData + index);
}

nautilus::val<uint64_t*> RecordBuffer::SequenceRangeRef::getStartPointer() const
{
    return seqStartData;
}

nautilus::val<uint64_t*> RecordBuffer::SequenceRangeRef::getEndPointer() const
{
    return seqEndData;
}

nautilus::val<size_t> RecordBuffer::SequenceRangeRef::getSize() const
{
    return size;
}

nautilus::val<Timestamp> RecordBuffer::getWatermarkTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getWatermark, tupleBufferRef)};
}

void RecordBuffer::setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setWatermark, tupleBufferRef, watermarkTs);
}

void RecordBuffer::setSequenceRange(SequenceRangeRef rangeRef)
{
    auto buffer = nautilus::invoke(+[](TupleBuffer* buffer) { return &buffer->getSequenceRange(); }, getReference());
    rangeRef.store(buffer);
}

RecordBuffer::SequenceRangeRef RecordBuffer::getSequenceRange() const
{
    auto buffer = nautilus::invoke(+[](TupleBuffer* buffer) { return &buffer->getSequenceRange(); }, getReference());
    return SequenceRangeRef::from(buffer);
}

nautilus::val<Timestamp> RecordBuffer::getCreatingTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getCreationTimestampInMS, tupleBufferRef)};
}

void RecordBuffer::setCreationTs(const nautilus::val<Timestamp>& creationTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setCreationTimestampInMS, tupleBufferRef, creationTs);
}

}
