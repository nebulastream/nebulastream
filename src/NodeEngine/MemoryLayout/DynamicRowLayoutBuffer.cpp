

#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>


namespace NES::NodeEngine {

uint64_t DynamicRowLayoutBuffer::calcOffset(uint64_t ithRecord, uint64_t jthField) {

    auto fieldOffSets = dynamicRowLayout->getFieldSizesOffsets();
    auto recordSize = dynamicRowLayout->getRecordSize();
    NES_VERIFY(jthField < fieldOffSets->size(), "jthField" << jthField << " is larger than fieldOffsets.size() " << fieldOffSets->size());

    return (ithRecord * recordSize) + fieldOffSets->at(jthField);
}
DynamicRowLayoutBuffer::DynamicRowLayoutBuffer(std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, DynamicRowLayoutPtr dynamicRowLayout)
    : DynamicLayoutBuffer(tupleBuffer, capacity), dynamicRowLayout(dynamicRowLayout) {}
}