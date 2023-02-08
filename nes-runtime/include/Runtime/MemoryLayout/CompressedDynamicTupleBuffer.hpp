#ifndef NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
#define NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP

#include "DynamicTupleBuffer.hpp"

namespace NES::Runtime::MemoryLayouts {
/**
 * @brief Wrapper class to represent a DynamicTupleBuffer with additional objects for compression.
 */
class CompressedDynamicTupleBuffer : public DynamicTupleBuffer {
  public:
    explicit CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer);
};

}// namespace NES::Runtime::MemoryLayouts
#endif//NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
