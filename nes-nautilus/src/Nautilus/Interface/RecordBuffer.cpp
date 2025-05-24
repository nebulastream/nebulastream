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
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Nautilus/Interface/TupleBufferProxyFunctions.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus
{

RecordBuffer::RecordBuffer(const nautilus::val<Memory::TupleBuffer*>& tupleBufferRef) : tupleBufferRef(tupleBufferRef)
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

nautilus::val<int8_t*> RecordBuffer::getBuffer() const
{
    return invoke(ProxyFunctions::NES_Memory_TupleBuffer_getBuffer, tupleBufferRef);
}

const nautilus::val<Memory::TupleBuffer*>& RecordBuffer::getReference() const
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

void RecordBuffer::setUsedMemorySize(const nautilus::val<uint64_t>& usedMemorySize)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setUsedMemorySize, tupleBufferRef, usedMemorySize);
}

void RecordBuffer::setSequenceNumber(const nautilus::val<SequenceNumber>& seqNumber)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setSequenceNumber, tupleBufferRef, seqNumber);
}

void RecordBuffer::setChunkNumber(const nautilus::val<ChunkNumber>& chunkNumber)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setChunkNumber, tupleBufferRef, chunkNumber);
}

nautilus::val<ChunkNumber> RecordBuffer::getChunkNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getChunkNumber, tupleBufferRef)};
}

void RecordBuffer::setLastChunk(const nautilus::val<bool>& isLastChunk)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setLastChunk, tupleBufferRef, isLastChunk);
}

nautilus::val<bool> RecordBuffer::isLastChunk()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_isLastChunk, tupleBufferRef)};
}

nautilus::val<Timestamp> RecordBuffer::getWatermarkTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getWatermark, tupleBufferRef)};
}

void RecordBuffer::setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setWatermark, tupleBufferRef, watermarkTs);
}

nautilus::val<SequenceNumber> RecordBuffer::getSequenceNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getSequenceNumber, tupleBufferRef)};
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
