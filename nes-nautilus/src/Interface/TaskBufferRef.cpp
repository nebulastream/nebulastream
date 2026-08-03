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
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferProxyFunctions.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

TaskBufferRef::TaskBufferRef(NautilusBuffer buffer) : buffer(std::move(buffer))
{
}

nautilus::val<uint64_t> TaskBufferRef::getNumRecords() const
{
    return invoke(ProxyFunctions::NES_Memory_Buffer_getNumberOfTuples, buffer.asArg());
}

void TaskBufferRef::setNumRecords(const nautilus::val<uint64_t>& numRecordsValue)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setNumberOfTuples, buffer.asArg(), numRecordsValue);
}

NautilusBuffer& TaskBufferRef::getBuffer()
{
    return buffer;
}

const NautilusBuffer& TaskBufferRef::getBuffer() const
{
    return buffer;
}

nautilus::val<OriginId> TaskBufferRef::getOriginId()
{
    return {invoke(ProxyFunctions::NES_Memory_Buffer_getOriginId, buffer.asArg())};
}

void TaskBufferRef::setOriginId(const nautilus::val<OriginId>& originId)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setOriginId, buffer.asArg(), originId);
}

void TaskBufferRef::setSequenceNumber(const nautilus::val<SequenceNumber>& seqNumber)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setSequenceNumber, buffer.asArg(), seqNumber);
}

void TaskBufferRef::setChunkNumber(const nautilus::val<ChunkNumber>& chunkNumber)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setChunkNumber, buffer.asArg(), chunkNumber);
}

nautilus::val<ChunkNumber> TaskBufferRef::getChunkNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_Buffer_getChunkNumber, buffer.asArg())};
}

void TaskBufferRef::setLastChunk(const nautilus::val<bool>& isLastChunk)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setLastChunk, buffer.asArg(), isLastChunk);
}

nautilus::val<bool> TaskBufferRef::isLastChunk()
{
    return {invoke(ProxyFunctions::NES_Memory_Buffer_isLastChunk, buffer.asArg())};
}

nautilus::val<Timestamp> TaskBufferRef::getWatermarkTs()
{
    return {invoke(ProxyFunctions::NES_Memory_Buffer_getWatermark, buffer.asArg())};
}

void TaskBufferRef::setWatermarkTs(const nautilus::val<Timestamp>& watermarkTs)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setWatermark, buffer.asArg(), watermarkTs);
}

nautilus::val<SequenceNumber> TaskBufferRef::getSequenceNumber()
{
    return {invoke(ProxyFunctions::NES_Memory_Buffer_getSequenceNumber, buffer.asArg())};
}

nautilus::val<Timestamp> TaskBufferRef::getCreatingTs()
{
    return {invoke(ProxyFunctions::NES_Memory_Buffer_getCreationTimestampInMS, buffer.asArg())};
}

void TaskBufferRef::setCreationTs(const nautilus::val<Timestamp>& creationTs)
{
    invoke(ProxyFunctions::NES_Memory_Buffer_setCreationTimestampInMS, buffer.asArg(), creationTs);
}

}
