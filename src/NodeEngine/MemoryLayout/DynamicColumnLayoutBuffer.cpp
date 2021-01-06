//
// Created by nils on 05.01.21.
//

#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>

namespace NES::NodeEngine {
uint64_t NES::NodeEngine::DynamicColumnLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField) {
    auto fieldSize = dynamicColLayout->getFieldSizes();
    NES_VERIFY(jthField < fieldSize->size(), "jthField" << jthField << " is larger than fieldSize.size() " << fieldSize->size());
    NES_VERIFY(jthField < columnOffsets.size(), "columnOffsets" << jthField << " is larger than columnOffsets.size() " << columnOffsets.size());

    auto offSet = (ithRecord * fieldSize->at(jthField)) + columnOffsets[jthField];
    NES_DEBUG("DynamicColumnLayoutBuffer.calcOffset: offSet = " << offSet);
    return offSet;
}
DynamicColumnLayoutBuffer::DynamicColumnLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity,
                                                     std::shared_ptr<DynamicColumnLayout> dynamicColLayout, std::vector<COL_OFFSET_SIZE> columnOffsets)
                                                    : DynamicLayoutBuffer(tupleBuffer, capacity), columnOffsets(std::move(columnOffsets)), dynamicColLayout(dynamicColLayout) {}


}