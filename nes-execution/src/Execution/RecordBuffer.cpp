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
#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

namespace NES::Runtime::Execution {

RecordBuffer::RecordBuffer(const Nautilus::MemRefVal& tupleBufferRef) : tupleBufferRef(tupleBufferRef) {}

Nautilus::UInt64Val RecordBuffer::getNumRecords() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples, tupleBufferRef);
}

void RecordBuffer::setNumRecords(const Nautilus::UInt64Val& numRecordsValue) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples, tupleBufferRef, numRecordsValue);
}

Nautilus::MemRefVal RecordBuffer::getBuffer() const {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer, tupleBufferRef);
}
const Nautilus::MemRefVal& RecordBuffer::getReference() const { return tupleBufferRef; }

Nautilus::UInt64Val RecordBuffer::getOriginId() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getOriginId, tupleBufferRef);
}

void RecordBuffer::setOriginId(const Nautilus::UInt64Val& originId) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setOriginId, tupleBufferRef, originId);
}

Nautilus::UInt64Val RecordBuffer::getStatisticId() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getStatisticId, tupleBufferRef);
}

void RecordBuffer::setStatisticId(const Nautilus::UInt64Val& statisticId) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setStatisticId, tupleBufferRef, statisticId);
}

void RecordBuffer::setSequenceNr(const Nautilus::UInt64Val& seqNumber) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setSequenceNumber, tupleBufferRef, seqNumber);
}

void RecordBuffer::setChunkNr(const Nautilus::UInt64Val& chunkNumber) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setChunkNumber, tupleBufferRef, chunkNumber);
}

Nautilus::UInt64Val RecordBuffer::getChunkNr() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getChunkNumber, tupleBufferRef);
}

void RecordBuffer::setLastChunk(const Nautilus::BooleanVal& isLastChunk) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setLastChunk, tupleBufferRef, isLastChunk);
}

Nautilus::BooleanVal RecordBuffer::isLastChunk() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__isLastChunk, tupleBufferRef);
}

Nautilus::UInt64Val RecordBuffer::getWatermarkTs() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getWatermark, tupleBufferRef);
}

void RecordBuffer::setWatermarkTs(const Nautilus::UInt64Val& watermarkTs) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setWatermark, tupleBufferRef, watermarkTs);
}

Nautilus::UInt64Val RecordBuffer::getSequenceNr() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getSequenceNumber, tupleBufferRef);
}

Nautilus::UInt64Val RecordBuffer::getCreatingTs() {
    return invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getCreationTimestampInMS, tupleBufferRef);
}

void RecordBuffer::setCreationTs(const Nautilus::UInt64Val& creationTs) {
    invoke(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setCreationTimestampInMS, tupleBufferRef, creationTs);
}

}// namespace NES::Runtime::Execution
