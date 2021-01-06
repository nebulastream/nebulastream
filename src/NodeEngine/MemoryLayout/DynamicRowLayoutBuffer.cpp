

#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>


namespace NES::NodeEngine {

uint64_t DynamicRowLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField) {

    auto fieldOffSets = dynamicRowLayout->getFieldOffSets();
    auto recordSize = dynamicRowLayout->getRecordSize();
    NES_VERIFY(jthField < fieldOffSets->size(), "jthField" << jthField << " is larger than fieldOffsets.size() " << fieldOffSets->size());

    auto offSet = (ithRecord * recordSize) + fieldOffSets->at(jthField);
    NES_DEBUG("DynamicColumnLayoutBuffer.calcOffset: offSet = " << offSet);
    return offSet;
}
DynamicRowLayoutBuffer::DynamicRowLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicRowLayoutPtr dynamicRowLayout)
    : DynamicLayoutBuffer(tupleBuffer, capacity), dynamicRowLayout(dynamicRowLayout) {}
}