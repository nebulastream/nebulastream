//
// Created by nils on 05.01.21.
//

#include "NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp"

namespace NES {

uint64_t DynamicRowLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField) {
    NES_VERIFY(jthField < fieldSizes->size(), "jthField" << jthField << " is larger than fieldSizes.size() " << fieldSizes->size());

    return (ithRecord * recordSize) + fieldSizes->at(jthField);
}

DynamicRowLayoutBuffer::DynamicRowLayoutBuffer(uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes)
    : recordSize(recordSize), fieldSizes(fieldSizes) {
    this->tupleBuffer = tupleBuffer;
}
}