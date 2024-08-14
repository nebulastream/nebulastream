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
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

namespace NES::Runtime::Execution {

RecordBuffer::RecordBuffer(const Nautilus::MemRef& tupleBufferRef) : tupleBufferRef(tupleBufferRef) {}

val<uint64_t> RecordBuffer::getNumRecords() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples, tupleBufferRef);
}

void RecordBuffer::setNumRecords(const val<uint64_t>& numRecordsValue) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples, tupleBufferRef, numRecordsValue);
}

Nautilus::MemRef RecordBuffer::getBuffer() const {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer, tupleBufferRef);
}
const val<int8_t*>& RecordBuffer::getReference() const { return tupleBufferRef; }

val<uint64_t> RecordBuffer::getOriginId() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getOriginId, tupleBufferRef);
}

void RecordBuffer::setOriginId(const val<uint64_t>& originId) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setOriginId, tupleBufferRef, originId);
}

val<uint64_t> RecordBuffer::getStatisticId() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getStatisticId, tupleBufferRef);
}

void RecordBuffer::setStatisticId(const val<uint64_t>& statisticId) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setStatisticId, tupleBufferRef, statisticId);
}

void RecordBuffer::setSequenceNr(const val<uint64_t>& seqNumber) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setSequenceNumber, tupleBufferRef, seqNumber);
}

void RecordBuffer::setChunkNr(const val<uint64_t>& chunkNumber) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setChunkNumber, tupleBufferRef, chunkNumber);
}

val<uint64_t> RecordBuffer::getChunkNr() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getChunkNumber, tupleBufferRef);
}

void RecordBuffer::setLastChunk(const val<bool>& isLastChunk) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setLastChunk, tupleBufferRef, isLastChunk);
}

val<bool> RecordBuffer::isLastChunk() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__isLastChunk, tupleBufferRef);
}

val<uint64_t> RecordBuffer::getWatermarkTs() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getWatermark, tupleBufferRef);
}

void RecordBuffer::setWatermarkTs(const val<uint64_t>& watermarkTs) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setWatermark, tupleBufferRef, watermarkTs);
}

val<uint64_t> RecordBuffer::getSequenceNr() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getSequenceNumber, tupleBufferRef);
}

val<uint64_t> RecordBuffer::getCreatingTs() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getCreationTimestampInMS, tupleBufferRef);
}

void RecordBuffer::setCreationTs(const val<uint64_t>& creationTs) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setCreationTimestampInMS, tupleBufferRef, creationTs);
}

}// namespace NES::Runtime::Execution
