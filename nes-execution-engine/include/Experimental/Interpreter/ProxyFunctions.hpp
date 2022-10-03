/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Runtime/TupleBuffer.hpp>
namespace NES::Runtime::ProxyFunctions {
// void* NES__Runtime__TupleBuffer__getBuffer(void* thisPtr) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->getBuffer();
// };
// uint64_t NES__Runtime__TupleBuffer__getBufferSize(void* thisPtr) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->getBufferSize();
// };
// uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void* thisPtr) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->getNumberOfTuples();
// };
// // void NES__Runtime__TupleBuffer__setNumberOfTuples(void* thisPtr, uint64_t numberOfTuples) {
// //     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
// //     return thisPtr_->setNumberOfTuples(numberOfTuples);
// // };
// extern "C" __attribute__((always_inline)) void NES__Runtime__TupleBuffer__setNumberOfTuples(void *thisPtr, uint64_t numberOfTuples) {
//    NES::Runtime::TupleBuffer *tupleBuffer = static_cast<NES::Runtime::TupleBuffer*>(thisPtr);
//    tupleBuffer->setNumberOfTuples(numberOfTuples);
// }
// uint64_t NES__Runtime__TupleBuffer__getWatermark(void* thisPtr) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->getWatermark();
// };
// void NES__Runtime__TupleBuffer__setWatermark(void* thisPtr, uint64_t value) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->setWatermark(value);
// };
// uint64_t NES__Runtime__TupleBuffer__getCreationTimestamp(void* thisPtr) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->getCreationTimestamp();
// };
// void NES__Runtime__TupleBuffer__setSequenceNumber(void* thisPtr, uint64_t sequenceNumber) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->setSequenceNumber(sequenceNumber);
// };
// uint64_t NES__Runtime__TupleBuffer__getSequenceNumber(void* thisPtr) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->getSequenceNumber();
// }
// void NES__Runtime__TupleBuffer__setCreationTimestamp(void* thisPtr, uint64_t value) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->setCreationTimestamp(value);
// }



//==--------------------------------------------------------------------==//
//==--------------------- CUSTOM PROXY FUNCTIONS -----------------------==//
//==--------------------------------------------------------------------==//
extern "C" __attribute__((always_inline))  void* NES__Runtime__TupleBuffer__getBuffer(void* thisPtr);
extern "C"  uint64_t NES__Runtime__TupleBuffer__getBufferSize(void* thisPtr);
extern "C" __attribute__((always_inline))  uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void* thisPtr);
extern "C" __attribute__((always_inline)) void NES__Runtime__TupleBuffer__setNumberOfTuples(void *thisPtr, uint64_t numberOfTuples);
extern "C"  uint64_t NES__Runtime__TupleBuffer__getWatermark(void* thisPtr);
extern "C"  void NES__Runtime__TupleBuffer__setWatermark(void* thisPtr, uint64_t value);
extern "C"  uint64_t NES__Runtime__TupleBuffer__getCreationTimestamp(void* thisPtr);
extern "C"  void NES__Runtime__TupleBuffer__setSequenceNumber(void* thisPtr, uint64_t sequenceNumber);
extern "C"  uint64_t NES__Runtime__TupleBuffer__getSequenceNumber(void* thisPtr);
extern "C"  void NES__Runtime__TupleBuffer__setCreationTimestamp(void* thisPtr, uint64_t value);

extern "C" __attribute__((always_inline)) void printInt64(int64_t inputValue);
extern "C" __attribute__((always_inline)) void stringToUpperCase(int64_t i, void *inputString);
extern "C" __attribute__((always_inline)) void stringToUpperCaseConstSize(int64_t i, void *inputString, int64_t stringSize);

extern "C" __attribute__((always_inline)) int64_t getMurMurHash(uint64_t inputValue);

extern "C" __attribute__((always_inline)) double standardDeviationGetMean(int64_t size, void *bufferPtr);
extern "C" __attribute__((always_inline)) double standardDeviationGetVariance(double runningDeviationSum, double mean, int64_t inputValue);
extern "C" __attribute__((always_inline)) double standardDeviationGetStdDev(double deviationSum, int64_t size);

extern "C" __attribute__((__always_inline__, __nodebug__, __target__("sse4.2"))) int64_t getCRC32Hash(uint64_t inputValue, uint64_t seed);


}// namespace NES::Runtime::ProxyFunctions