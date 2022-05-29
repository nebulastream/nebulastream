
#include <Runtime/TupleBuffer.hpp>
namespace NES::Runtime::ProxyFunctions {
uint8_t * NES__Runtime__TupleBuffer__getBuffer(void * thisPtr){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->getBuffer();
}
;
uint64_t NES__Runtime__TupleBuffer__getBufferSize(void * thisPtr){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->getBufferSize();
}
;
uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void * thisPtr){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->getNumberOfTuples();
}
;
void NES__Runtime__TupleBuffer__setNumberOfTuples(void * thisPtr,uint64_t numberOfTuples){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->setNumberOfTuples(numberOfTuples);
}
;
uint64_t NES__Runtime__TupleBuffer__getWatermark(void * thisPtr){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->getWatermark();
}
;
void NES__Runtime__TupleBuffer__setWatermark(void * thisPtr,uint64_t value){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->setWatermark(value);
}
;
uint64_t NES__Runtime__TupleBuffer__getCreationTimestamp(void * thisPtr){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->getCreationTimestamp();
}
;
void NES__Runtime__TupleBuffer__setSequenceNumber(void * thisPtr,uint64_t sequenceNumber){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->setSequenceNumber(sequenceNumber);
}
;
uint64_t NES__Runtime__TupleBuffer__getSequenceNumber(void * thisPtr){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->getSequenceNumber();
}
void NES__Runtime__TupleBuffer__setCreationTimestamp(void * thisPtr,uint64_t value){
auto* thisPtr_ =(NES::Runtime::TupleBuffer*)thisPtr;
return thisPtr_->setCreationTimestamp(value);
}

}