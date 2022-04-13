
#include <Runtime/TupleBuffer.hpp>
extern "C" __attribute__((always_inline)) uint64_t getNumTuples(void *tupleBufferPointer) {
   NES::Runtime::TupleBuffer *tupleBuffer = static_cast<NES::Runtime::TupleBuffer*>(tupleBufferPointer);
   return tupleBuffer->getNumberOfTuples();
}