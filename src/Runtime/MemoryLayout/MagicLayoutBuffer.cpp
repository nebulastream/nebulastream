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
#include <Runtime/MemoryLayout/MagicLayoutBuffer.hpp>
#include <utility>
namespace NES::Runtime::DynamicMemoryLayout {

DynamicField::DynamicField(uint8_t* address) : address(address) {}

DynamicField DynamicRecord::operator[](std::size_t fieldIndex) const {
    auto* bufferBasePointer = buffer->getTupleBuffer().getBuffer<uint8_t>();
    auto offset = buffer->calcOffset(recordIndex, fieldIndex, true);
    auto* basePointer = bufferBasePointer + offset;
    return DynamicField{basePointer};
}

DynamicRecord::DynamicRecord(uint64_t recordIndex, DynamicLayoutBufferPtr buffer)
    : recordIndex(recordIndex), buffer(std::move(buffer)){};

uint64_t MagicLayoutBuffer::getCapacity() const { return buffer->getCapacity(); }

uint64_t MagicLayoutBuffer::getNumberOfRecords() const { return buffer->getNumberOfRecords(); }

DynamicRecord MagicLayoutBuffer::operator[](std::size_t recordIndex) {
    if (recordIndex >= getCapacity()) {
        throw BufferAccessException("index is out of bound");
    }
    return {recordIndex, buffer};
}

MagicLayoutBuffer::MagicLayoutBuffer(std::shared_ptr<DynamicLayoutBuffer> buffer) : buffer(std::move(buffer)) {}

MagicLayoutBuffer::RecordIterator::RecordIterator(MagicLayoutBuffer& buffer) : RecordIterator(buffer, 0) {}

MagicLayoutBuffer::RecordIterator::RecordIterator(MagicLayoutBuffer& buffer, uint64_t currentIndex)
    : buffer(buffer), currentIndex(currentIndex) {}

MagicLayoutBuffer::RecordIterator& MagicLayoutBuffer::RecordIterator::operator++() {
    currentIndex++;
    return *this;
}

const MagicLayoutBuffer::RecordIterator MagicLayoutBuffer::RecordIterator::operator++(int) {
    RecordIterator retval = *this;
    ++(*this);
    return retval;
}

bool MagicLayoutBuffer::RecordIterator::operator==(RecordIterator other) const {
    return currentIndex == other.currentIndex && &buffer == &other.buffer;
}

bool MagicLayoutBuffer::RecordIterator::operator!=(RecordIterator other) const { return !(*this == other); }

DynamicRecord MagicLayoutBuffer::RecordIterator::operator*() const { return buffer[currentIndex]; }

}// namespace NES::Runtime::DynamicMemoryLayout