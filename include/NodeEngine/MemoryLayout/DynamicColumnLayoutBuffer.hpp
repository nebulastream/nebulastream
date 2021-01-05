//
// Created by nils on 05.01.21.
//

#ifndef NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
#define NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP

#include <stdint.h>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>


namespace NES::NodeEngine {

typedef uint64_t FIELD_SIZE;
typedef uint64_t COL_OFFSET_SIZE;

class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer{

  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicColumnLayoutBuffer(uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes, std::shared_ptr<std::vector<COL_OFFSET_SIZE>> columnOffsets,
                              std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, bool checkBoundaryFieldChecks);
    template<typename  T> std::tuple<T> readRecord(uint64_t recordIndex);
    template<typename  T> void pushRecord(std::tuple<T> record);

  private:
    std::shared_ptr<std::vector<COL_OFFSET_SIZE>> columnOffsets;

};

}

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
