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
#include <Interface/TaskBufferRef.hpp>

#include <cstdint>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Interface/TupleBufferProxyFunctions.hpp>
#include <Runtime/Buffer.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

TaskBufferRef::TaskBufferRef(const nautilus::val<Buffer*>& tupleBufferRef) : tupleBufferRef(tupleBufferRef)
{
}

nautilus::val<uint64_t> TaskBufferRef::getNumRecords() const
{
    return invoke(ProxyFunctions::NES_Memory_TupleBuffer_getNumberOfTuples, tupleBufferRef);
}

void TaskBufferRef::setNumRecords(const nautilus::val<uint64_t>& numRecordsValue)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setNumberOfTuples, tupleBufferRef, numRecordsValue);
}

nautilus::val<int8_t*> TaskBufferRef::getMemArea() const
{
    return invoke(ProxyFunctions::NES_Memory_TupleBuffer_getMemArea, tupleBufferRef);
}

const nautilus::val<Buffer*>& TaskBufferRef::getReference() const
{
    return tupleBufferRef;
}

nautilus::val<OriginId> TaskBufferRef::getOriginId()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getOriginId, tupleBufferRef)};
}

void TaskBufferRef::setOriginId(const nautilus::val<OriginId>& originId)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setOriginId, tupleBufferRef, originId);
}

void TaskBufferRef::setSequenceNumber(const nautilus::val<SequenceNumber>& seqNumber)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setSequenceNumber, tupleBufferRef, seqNumber);
}

void TaskBufferRef::setChunkNumber(const nautilus::val<ChunkNumber>& chunkNumber)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setChunkNumber, tupleBufferRef, chunkNumber);
}

nautilus::val<ChunkNumber> TaskBufferRef::getChunkNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getChunkNumber, tupleBufferRef)};
}

void TaskBufferRef::setLastChunk(const nautilus::val<bool>& isLastChunk)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setLastChunk, tupleBufferRef, isLastChunk);
}

nautilus::val<bool> TaskBufferRef::isLastChunk()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_isLastChunk, tupleBufferRef)};
}

nautilus::val<Timestamp> TaskBufferRef::getWatermarkTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getWatermark, tupleBufferRef)};
}

void TaskBufferRef::setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setWatermark, tupleBufferRef, watermarkTs);
}

nautilus::val<SequenceNumber> TaskBufferRef::getSequenceNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getSequenceNumber, tupleBufferRef)};
}

nautilus::val<Timestamp> TaskBufferRef::getCreatingTs()
{
    return {invoke(ProxyFunctions::NES_Memory_TupleBuffer_getCreationTimestampInMS, tupleBufferRef)};
}

void TaskBufferRef::setCreationTs(const nautilus::val<Timestamp>& creationTs)
{
    invoke(ProxyFunctions::NES_Memory_TupleBuffer_setCreationTimestampInMS, tupleBufferRef, creationTs);
}

}
