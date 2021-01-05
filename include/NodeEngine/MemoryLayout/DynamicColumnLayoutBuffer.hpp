//
// Created by nils on 05.01.21.
//

#ifndef NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
#define NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP

#include <stdint.h>
#include "DynamicLayoutBuffer.hpp"


namespace NES {

typedef uint64_t FIELD_SIZE;
typedef uint64_t COL_OFFSET_SIZE;

class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer{

  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;

  private:
    std::vector<COL_OFFSET_SIZE> columnOffsets;
    std::vector<FIELD_SIZE> fieldSizes;

};

}

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
