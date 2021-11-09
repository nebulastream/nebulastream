/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Exceptions/BufferAccessException.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <utility>
namespace NES::Runtime::MemoryLayouts {

DynamicField::DynamicField(uint8_t* address) : address(address) {}

DynamicField DynamicTuple::operator[](std::size_t fieldIndex) {
    auto* bufferBasePointer = buffer.getBuffer<uint8_t>();
    auto offset = memoryLayout->getFieldOffset(recordIndex, fieldIndex);
    auto* basePointer = bufferBasePointer + offset;
    return DynamicField{basePointer};
}

DynamicTuple::DynamicTuple(const uint64_t recordIndex, MemoryLayoutPtr memoryLayout, TupleBuffer& buffer)
    : recordIndex(recordIndex), memoryLayout(std::move(memoryLayout)), buffer(std::move(buffer)){};

uint64_t DynamicTupleBuffer::getCapacity() const { return memoryLayout->getCapacity(); }

uint64_t DynamicTupleBuffer::getNumberOfTuples() const { return buffer.getNumberOfTuples(); }

void DynamicTupleBuffer::setNumberOfTuples(uint64_t value) { buffer.setNumberOfTuples(value); }

DynamicTuple DynamicTupleBuffer::operator[](std::size_t recordIndex) {
    if (recordIndex >= getCapacity()) {
        throw BufferAccessException("index " + std::to_string(recordIndex) + " is out of bound");
    }
    return {recordIndex, memoryLayout, buffer};
}

DynamicTupleBuffer::DynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer& buffer)
    : memoryLayout(memoryLayout), buffer(buffer) {}

DynamicTupleBuffer::RecordIterator::RecordIterator(DynamicTupleBuffer& buffer) : RecordIterator(buffer, 0) {}

DynamicTupleBuffer::RecordIterator::RecordIterator(DynamicTupleBuffer& buffer, uint64_t currentIndex)
    : buffer(buffer), currentIndex(currentIndex) {}

DynamicTupleBuffer::RecordIterator& DynamicTupleBuffer::RecordIterator::operator++() {
    currentIndex++;
    return *this;
}

const DynamicTupleBuffer::RecordIterator DynamicTupleBuffer::RecordIterator::operator++(int) {
    RecordIterator retval = *this;
    ++(*this);
    return retval;
}

bool DynamicTupleBuffer::RecordIterator::operator==(RecordIterator other) const {
    return currentIndex == other.currentIndex && &buffer == &other.buffer;
}

bool DynamicTupleBuffer::RecordIterator::operator!=(RecordIterator other) const { return !(*this == other); }

DynamicTuple DynamicTupleBuffer::RecordIterator::operator*() const { return buffer[currentIndex]; }

}// namespace NES::Runtime::MemoryLayouts