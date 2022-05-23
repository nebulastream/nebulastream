#include <Interpreter/Record.hpp>
#include <Interpreter/RecordBuffer.hpp>

#include <Interpreter/FunctionCall.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Interpreter/Operations/AndOp.hpp>
#include <Interpreter/Operations/DivOp.hpp>
#include <Interpreter/Operations/EqualsOp.hpp>
#include <Interpreter/Operations/LessThenOp.hpp>
#include <Interpreter/Operations/MulOp.hpp>
#include <Interpreter/Operations/NegateOp.hpp>
#include <Interpreter/Operations/OrOp.hpp>
#include <Interpreter/Operations/SubOp.hpp>
#include <ProxyFunctions.hpp>

namespace NES::Interpreter {

RecordBuffer::RecordBuffer(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<MemRef> tupleBufferRef)
    : memoryLayout(memoryLayout), tupleBufferRef(tupleBufferRef) {}

Value<Integer> RecordBuffer::getNumRecords() {
    return FunctionCall<>(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples, tupleBufferRef);
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
    FunctionCall<>(Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples, tupleBufferRef, value);
}

}// namespace NES::Interpreter