

#ifndef NES_DYNAMICROWLAYOUTBUFFER_HPP
#define NES_DYNAMICROWLAYOUTBUFFER_HPP

#include <stdint.h>
#include "DynamicLayoutBuffer.hpp"

namespace NES {

typedef uint64_t FIELD_SIZE;


class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicRowLayoutBuffer(uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes);

  private:
    uint64_t recordSize;
    std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes;
};
}

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
