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
#include <Interface/RecordBuffer.hpp>

#include <cstdint>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Interface/TupleBufferProxyFunctions.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

RecordBuffer::RecordBuffer(NautilusBuffer buffer) : buffer{std::move(buffer)}
{
}

nautilus::val<uint64_t> RecordBuffer::getNumRecords() const
{
    return buffer.getNumberOfRecords();
}

void RecordBuffer::setNumRecords(const nautilus::val<uint64_t>& numRecordsValue)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setNumberOfTuples, buffer.asArg(), numRecordsValue);
}

nautilus::val<int8_t*> RecordBuffer::getMemArea() const
{
    return buffer.data();
}

nautilus::val<TupleBuffer*> RecordBuffer::getReference() const
{
    return buffer.asArg();
}

nautilus::val<OriginId> RecordBuffer::getOriginId()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getOriginId, buffer.asArg())};
}

void RecordBuffer::setOriginId(const nautilus::val<OriginId>& originId)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setOriginId, buffer.asArg(), originId);
}

void RecordBuffer::setSequenceNumber(const nautilus::val<SequenceNumber>& seqNumber)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setSequenceNumber, buffer.asArg(), seqNumber);
}

void RecordBuffer::setChunkNumber(const nautilus::val<ChunkNumber>& chunkNumber)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setChunkNumber, buffer.asArg(), chunkNumber);
}

nautilus::val<ChunkNumber> RecordBuffer::getChunkNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getChunkNumber, buffer.asArg())};
}

void RecordBuffer::setLastChunk(const nautilus::val<bool>& isLastChunk)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setLastChunk, buffer.asArg(), isLastChunk);
}

nautilus::val<bool> RecordBuffer::isLastChunk()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_isLastChunk, buffer.asArg())};
}

nautilus::val<Timestamp> RecordBuffer::getWatermarkTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getWatermark, buffer.asArg())};
}

void RecordBuffer::setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setWatermark, buffer.asArg(), watermarkTs);
}

nautilus::val<SequenceNumber> RecordBuffer::getSequenceNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getSequenceNumber, buffer.asArg())};
}

nautilus::val<Timestamp> RecordBuffer::getCreatingTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getCreationTimestampInMS, buffer.asArg())};
}

void RecordBuffer::setCreationTs(const nautilus::val<Timestamp>& creationTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setCreationTimestampInMS, buffer.asArg(), creationTs);
}

}
