

#ifndef NES_DYNAMICROWLAYOUTBUFFER_HPP
#define NES_DYNAMICROWLAYOUTBUFFER_HPP

#include <stdint.h>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>

namespace NES::NodeEngine {

class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicRowLayoutBuffer(uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes, std::shared_ptr<TupleBuffer> tupleBuffer,
                           uint64_t capacity, bool checkBoundaryFieldChecks);
    template<typename  T> std::tuple<T> readRecord(uint64_t recordIndex);
    template<typename  T> void pushRecord(std::tuple<T> record);

};

template<typename T>
std::tuple<T> DynamicRowLayoutBuffer::readRecord(uint64_t recordIndex) {
    NES_NOT_IMPLEMENTED();
    ((void) recordIndex);
    return std::tuple<T>();
}
template<typename T>
void DynamicRowLayoutBuffer::pushRecord(std::tuple<T> record) {
    ((void) record);
    NES_NOT_IMPLEMENTED();
}
}

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
