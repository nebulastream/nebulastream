//
// Created by nils on 05.01.21.
//

#ifndef NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
#define NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP

#include "DynamicColumnLayout.hpp"
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <stdint.h>

namespace NES::NodeEngine {

class DynamicColumnLayoutBuffer;
typedef std::unique_ptr<DynamicColumnLayoutBuffer> DynamicColumnLayoutBufferPtr;

typedef uint64_t COL_OFFSET_SIZE;

class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer{

  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicColumnLayoutBuffer(std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, DynamicColumnLayoutPtr dynamicColLayout, std::vector<COL_OFFSET_SIZE> columnOffsets);
    template<typename  T> std::tuple<T> readRecord(uint64_t recordIndex);
    template<typename  T> void pushRecord(std::tuple<T> record);

  private:
    std::vector<COL_OFFSET_SIZE> columnOffsets;
    DynamicColumnLayoutPtr dynamicColLayout;
};

template<typename T>
std::tuple<T> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    ((void) recordIndex);
    NES_NOT_IMPLEMENTED();
}
template<typename T>
void DynamicColumnLayoutBuffer::pushRecord(std::tuple<T> record) {
    ((void) record);
    NES_NOT_IMPLEMENTED();
}

}

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
