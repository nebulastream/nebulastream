
#ifndef NES_DYNAMICLAYOUTBUFFER_HPP
#define NES_DYNAMICLAYOUTBUFFER_HPP

#include <NodeEngine/TupleBuffer.hpp>
#include <string.h>

namespace NES::NodeEngine {

typedef uint64_t FIELD_SIZE;
typedef std::shared_ptr<std::vector<NES::NodeEngine::FIELD_SIZE>> FieldSizesPtr;


class DynamicLayoutBuffer {

  public:
    DynamicLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity)
        : tupleBuffer(tupleBuffer), capacity(capacity), numberOfRecords(0) {}
    /**
     * @brief calculates the address/offset of ithRecord and jthField
     * @param ithRecord
     * @param jthField
     * @return
     */
    virtual uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) = 0;
    uint64_t getCapacity() { return capacity; }
    uint64_t getNumberOfRecords() {return numberOfRecords; }

  protected:
    TupleBuffer& tupleBuffer;
    uint64_t capacity;
    uint64_t numberOfRecords;
};
}


#endif//NES_DYNAMICLAYOUTBUFFER_HPP
