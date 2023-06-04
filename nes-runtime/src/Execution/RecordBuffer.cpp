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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/TupleBufferProxyFunctions.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>

namespace NES::Runtime::Execution {

RecordBuffer::RecordBuffer(const Value<MemRef>& tupleBufferRef) : tupleBufferRef(tupleBufferRef) {}

Value<UInt64> RecordBuffer::getNumRecords() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getNumberOfTuples",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples,
                          tupleBufferRef);
}

void RecordBuffer::setNumRecords(const Value<UInt64>& numRecordsValue) {
    FunctionCall<>("NES__Runtime__TupleBuffer__setNumberOfTuples",
                   Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples,
                   tupleBufferRef,
                   numRecordsValue);
}

Value<MemRef> RecordBuffer::getBuffer() const {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getBuffer",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer,
                          tupleBufferRef);
}
const Value<MemRef>& RecordBuffer::getReference() const { return tupleBufferRef; }

Value<UInt64> RecordBuffer::getOriginId() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getOriginId",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getOriginId,
                          tupleBufferRef);
}

void RecordBuffer::setOriginId(const Value<UInt64>& originId) {
    FunctionCall<>("NES__Runtime__TupleBuffer__setOriginId",
                   Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setOriginId,
                   tupleBufferRef,
                   originId);
}

void RecordBuffer::setSequenceNr(const Value<UInt64>& seqNumber) {
    FunctionCall<>("NES__Runtime__TupleBuffer__setSequenceNr",
                   Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setSequenceNumber,
                   tupleBufferRef,
                   seqNumber);
}

Value<UInt64> RecordBuffer::getWatermarkTs() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__Watermark",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getWatermark,
                          tupleBufferRef);
}

void RecordBuffer::setWatermarkTs(const Value<UInt64>& watermarkTs) {
    FunctionCall<>("NES__Runtime__TupleBuffer__setWatermark",
                   Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setWatermark,
                   tupleBufferRef,
                   watermarkTs);
}

Value<UInt64> RecordBuffer::getSequenceNr() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getSequenceNumber",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getSequenceNumber,
                          tupleBufferRef);
}

Value<UInt64> RecordBuffer::getCreatingTs() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getCreationTimestampInMS",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getCreationTimestampInMS,
                          tupleBufferRef);
}

void RecordBuffer::setCreationTs(const Value<NES::Nautilus::UInt64>& creationTs) {
    FunctionCall<>("NES__Runtime__TupleBuffer__setWatermark",
                   Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setCreationTimestampInMS,
                   tupleBufferRef,
                   creationTs);
}

}// namespace NES::Runtime::Execution