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
#include <Experimental/Interpreter/Record.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operations/AddOp.hpp>
#include <Experimental/Interpreter/Operations/AndOp.hpp>
#include <Experimental/Interpreter/Operations/DivOp.hpp>
#include <Experimental/Interpreter/Operations/EqualsOp.hpp>
#include <Experimental/Interpreter/Operations/LessThenOp.hpp>
#include <Experimental/Interpreter/Operations/MulOp.hpp>
#include <Experimental/Interpreter/Operations/NegateOp.hpp>
#include <Experimental/Interpreter/Operations/OrOp.hpp>
#include <Experimental/Interpreter/Operations/SubOp.hpp>
#include <Experimental/Interpreter/ProxyFunctions.hpp>

namespace NES::Experimental::Interpreter {

RecordBuffer::RecordBuffer(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<MemRef> tupleBufferRef)
    : memoryLayout(memoryLayout), tupleBufferRef(tupleBufferRef) {}

Value<Integer> RecordBuffer::getNumRecords() {
    return FunctionCall<>("TupleBuffer.getNumberOfTuples",Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples, tupleBufferRef);
}

Record RecordBuffer::read(Value<Integer> recordIndex) {
    // read all fields
    // TODO add support for columnar layout
    auto fieldSizes = memoryLayout->getFieldSizes();
    auto tupleSize = memoryLayout->getTupleSize();
    std::vector<Value<Any>> fieldValues;
    for (auto fieldSize : fieldSizes) {
        auto fieldOffset = tupleSize * recordIndex + fieldSize;
        auto fieldAddress = this->tupleBufferRef + fieldOffset;
        auto memRef = fieldAddress.as<MemRef>();
        auto value = load<Integer>(memRef);
        fieldValues.emplace_back(value);
    }
    return Record(fieldValues);
}

void RecordBuffer::write(Value<Integer> recordIndex, Record& rec) {
    auto fieldSizes = memoryLayout->getFieldSizes();
    auto tupleSize = memoryLayout->getTupleSize();
    for (uint64_t i = 0; i < fieldSizes.size(); i++) {
        auto fieldOffset = tupleSize * recordIndex + fieldSizes[i];
        auto fieldAddress = this->tupleBufferRef + fieldOffset;
        auto value = rec.read(i).as<Integer>();
        auto memRef = fieldAddress.as<MemRef>();
        store<Integer>(memRef, value);
    }
}
void RecordBuffer::setNumRecords(Value<Integer> value) {
    FunctionCall<>("TupleBuffer.setNumberOfTuples",Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples, tupleBufferRef, value);
}

}// namespace NES::Experimental::Interpreter