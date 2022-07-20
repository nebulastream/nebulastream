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
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/ProxyFunctions.hpp>
#include <Experimental/Interpreter/Record.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

RecordBuffer::RecordBuffer(Value<MemRef> tupleBufferRef) : tupleBufferRef(tupleBufferRef) {}

Value<UInt64> RecordBuffer::getNumRecords() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getNumberOfTuples",
                          Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples,
                          tupleBufferRef);
}

Value<> load(PhysicalTypePtr type, Value<MemRef> memRef) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::INT_8: {
                return memRef.load<Int8>();
            };
            case BasicPhysicalType::INT_16: {
                return memRef.load<Int16>();
            };
            case BasicPhysicalType::INT_32: {
                return memRef.load<Int32>();
            };
            case BasicPhysicalType::INT_64: {
                return memRef.load<Int64>();
            };
            case BasicPhysicalType::UINT_8: {
                return memRef.load<UInt8>();
            };
            case BasicPhysicalType::UINT_16: {
                return memRef.load<UInt16>();
            };
            case BasicPhysicalType::UINT_32: {
                return memRef.load<UInt32>();
            };
            case BasicPhysicalType::UINT_64: {
                return memRef.load<UInt64>();
            };
            case BasicPhysicalType::FLOAT: {
                return memRef.load<Float>();
            };
            case BasicPhysicalType::DOUBLE: {
                return memRef.load<Double>();
            };
            default: {
                NES_NOT_IMPLEMENTED();
            };
        }
    }
    NES_NOT_IMPLEMENTED();
}

Record RecordBuffer::read(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                          Value<MemRef> bufferAddress,
                          Value<UInt64> recordIndex) {
    // read all fields
    // TODO add support for columnar layout
    auto rowLayout = std::dynamic_pointer_cast<Runtime::MemoryLayouts::RowLayout>(memoryLayout);
    auto tupleSize = rowLayout->getTupleSize();
    std::vector<Value<Any>> fieldValues;
    for (uint64_t i = 0; i < rowLayout->getSchema()->getSize(); i++) {
        auto fieldOffset = rowLayout->getFieldOffSets()[i];
        auto offset = tupleSize * recordIndex + fieldOffset;
        auto fieldAddress = bufferAddress + offset;
        auto memRef = fieldAddress.as<MemRef>();
        auto value = load(memoryLayout->getPhysicalTypes()[i], memRef);
        fieldValues.emplace_back(value);
    }
    return Record(fieldValues);
}

void RecordBuffer::write(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<UInt64> recordIndex, Record& rec) {
    auto fieldSizes = memoryLayout->getFieldSizes();
    auto tupleSize = memoryLayout->getTupleSize();
    auto bufferAddress = getBuffer();
    for (uint64_t i = 0; i < fieldSizes.size(); i++) {
        auto fieldOffset = tupleSize * recordIndex + fieldSizes[i];
        auto fieldAddress = bufferAddress + fieldOffset;
        auto value = rec.read(i).as<Int64>();
        auto memRef = fieldAddress.as<MemRef>();
        memRef.store(value);
    }
}
void RecordBuffer::setNumRecords(Value<UInt64> value) {
    FunctionCall<>("NES__Runtime__TupleBuffer__setNumberOfTuples",
                   Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples,
                   tupleBufferRef,
                   value);
}
Value<MemRef> RecordBuffer::getBuffer() {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getBuffer", Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer, tupleBufferRef);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter