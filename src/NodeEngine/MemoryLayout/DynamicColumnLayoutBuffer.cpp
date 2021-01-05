//
// Created by nils on 05.01.21.
//

#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>

namespace NES::NodeEngine {
uint64_t NES::NodeEngine::DynamicColumnLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField) {
    NES_VERIFY(jthField < fieldSizes->size(), "jthField" << jthField << " is larger than fieldSizes.size() " << fieldSizes->size());
    NES_VERIFY(jthField < columnOffsets->size(), "columnOffsets" << jthField << " is larger than columnOffsets.size() " << columnOffsets->size());

    return (ithRecord * fieldSizes->at(jthField)) + columnOffsets->at(jthField);
}


DynamicColumnLayoutBuffer::DynamicColumnLayoutBuffer(uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes, std::shared_ptr<std::vector<COL_OFFSET_SIZE>> columnOffsets,
                                                     std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, bool checkBoundaryFieldChecks)
        : columnOffsets(columnOffsets), DynamicLayoutBuffer(tupleBuffer, capacity, checkBoundaryFieldChecks, recordSize, fieldSizes) {}
}