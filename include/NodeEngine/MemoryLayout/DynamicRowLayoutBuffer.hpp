

#ifndef NES_DYNAMICROWLAYOUTBUFFER_HPP
#define NES_DYNAMICROWLAYOUTBUFFER_HPP

#include <stdint.h>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>

namespace NES::NodeEngine {

class DynamicRowLayoutBuffer;
typedef std::unique_ptr<DynamicRowLayoutBuffer> DynamicRowLayoutBufferPtr;

class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicRowLayoutBuffer(std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, DynamicRowLayoutPtr dynamicRowLayout);

    template<typename  T> std::tuple<T> readRecord(uint64_t recordIndex);
    template<typename  T> void pushRecord(std::tuple<T> record);

  private:
    DynamicRowLayoutPtr dynamicRowLayout;
};

template<typename T>
std::tuple<T> DynamicRowLayoutBuffer::readRecord(uint64_t recordIndex) {
    ((void) recordIndex);
    NES_NOT_IMPLEMENTED();
}
template<typename T>
void DynamicRowLayoutBuffer::pushRecord(std::tuple<T> record) {
    ((void) record);
    NES_NOT_IMPLEMENTED();
}

}

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
