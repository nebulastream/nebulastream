

#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>

namespace NES::NodeEngine {

uint64_t DynamicRowLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField) {
    NES_VERIFY(jthField < fieldSizes->size(), "jthField" << jthField << " is larger than fieldSizes.size() " << fieldSizes->size());

    return (ithRecord * recordSize) + fieldSizes->at(jthField);
}

DynamicRowLayoutBuffer::DynamicRowLayoutBuffer(uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes, std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, bool checkBoundaryFieldChecks)
    : DynamicLayoutBuffer(tupleBuffer, capacity, checkBoundaryFieldChecks, recordSize, fieldSizes) {}
}