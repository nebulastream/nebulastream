
#ifndef NES_DYNAMICLAYOUTBUFFER_HPP
#define NES_DYNAMICLAYOUTBUFFER_HPP

#include <NodeEngine/TupleBuffer.hpp>
namespace NES {
class DynamicLayoutBuffer {

  public:
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
    TupleBuffer tupleBuffer;
    bool checkBoundaryFieldChecks;
    uint64_t capacity;
    uint64_t numberOfRecords;
};
}


#endif//NES_DYNAMICLAYOUTBUFFER_HPP
