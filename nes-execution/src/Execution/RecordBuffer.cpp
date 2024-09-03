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
#include <Execution/RecordBuffer.hpp>
#include <Execution/TupleBufferProxyFunctions.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution
{

RecordBuffer::RecordBuffer(const nautilus::val<int8_t*>& tupleBufferRef) : tupleBufferRef(tupleBufferRef)
{
}

nautilus::val<uint64_t> RecordBuffer::getNumRecords()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getNumberOfTuples, tupleBufferRef);
}

void RecordBuffer::setNumRecords(const nautilus::val<uint64_t>& numRecordsValue)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setNumberOfTuples, tupleBufferRef, numRecordsValue);
}

nautilus::val<int8_t*> RecordBuffer::getBuffer() const
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getBuffer, tupleBufferRef);
}
const nautilus::val<int8_t*>& RecordBuffer::getReference() const
{
    return tupleBufferRef;
}

nautilus::val<uint64_t> RecordBuffer::getOriginId()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getOriginId, tupleBufferRef);
}

void RecordBuffer::setOriginId(const nautilus::val<uint64_t>& originId)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setOriginId, tupleBufferRef, originId);
}

void RecordBuffer::setSequenceNr(const nautilus::val<uint64_t>& seqNumber)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setSequenceNumber, tupleBufferRef, seqNumber);
}

void RecordBuffer::setChunkNr(const nautilus::val<uint64_t>& chunkNumber)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setChunkNumber, tupleBufferRef, chunkNumber);
}

nautilus::val<uint64_t> RecordBuffer::getChunkNr()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getChunkNumber, tupleBufferRef);
}

void RecordBuffer::setLastChunk(const nautilus::val<bool>& isLastChunk)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setLastChunk, tupleBufferRef, isLastChunk);
}

nautilus::val<bool> RecordBuffer::isLastChunk()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__isLastChunk, tupleBufferRef);
}

nautilus::val<uint64_t> RecordBuffer::getWatermarkTs()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getWatermark, tupleBufferRef);
}

void RecordBuffer::setWatermarkTs(const nautilus::val<uint64_t>& watermarkTs)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setWatermark, tupleBufferRef, watermarkTs);
}

nautilus::val<uint64_t> RecordBuffer::getSequenceNr()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getSequenceNumber, tupleBufferRef);
}

nautilus::val<uint64_t> RecordBuffer::getCreatingTs()
{
    return nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__getCreationTimestampInMS, tupleBufferRef);
}

void RecordBuffer::setCreationTs(const nautilus::val<uint64_t>& creationTs)
{
    nautilus::invoke(Runtime::ProxyFunctions::NES__Memory__TupleBuffer__setCreationTimestampInMS, tupleBufferRef, creationTs);
}

} /// namespace NES::Runtime::Execution
