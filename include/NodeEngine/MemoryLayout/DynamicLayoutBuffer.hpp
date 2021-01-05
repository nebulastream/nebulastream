
#ifndef NES_DYNAMICLAYOUTBUFFER_HPP
#define NES_DYNAMICLAYOUTBUFFER_HPP

#include <NodeEngine/TupleBuffer.hpp>

namespace NES::NodeEngine {

typedef uint64_t FIELD_SIZE;


class DynamicLayoutBuffer {

  public:
    DynamicLayoutBuffer(std::shared_ptr<TupleBuffer> tupleBuffer, uint64_t capacity, bool checkBoundaryFieldChecks, uint64_t recordSize, std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes)
        : tupleBuffer(tupleBuffer), checkBoundaryFieldChecks(checkBoundaryFieldChecks), capacity(capacity), numberOfRecords(0), recordSize(recordSize), fieldSizes(fieldSizes) {}
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
    std::shared_ptr<TupleBuffer> tupleBuffer;
    bool checkBoundaryFieldChecks;
    uint64_t capacity;
    uint64_t numberOfRecords;
    uint64_t recordSize;
    std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes;

};
}


#endif//NES_DYNAMICLAYOUTBUFFER_HPP
