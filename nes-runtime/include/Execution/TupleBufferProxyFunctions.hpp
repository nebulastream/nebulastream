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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_TUPLEBUFFERPROXYFUNCTIONS_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_TUPLEBUFFERPROXYFUNCTIONS_HPP_

#include <Runtime/TupleBuffer.hpp>
namespace NES::Runtime::ProxyFunctions {

void* NES__Runtime__TupleBuffer__getBuffer(void* thisPtr);

uint64_t NES__Runtime__TupleBuffer__getBufferSize(void* thisPtr);

uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void* thisPtr);

extern "C" __attribute__((always_inline)) void NES__Runtime__TupleBuffer__setNumberOfTuples(void* thisPtr,
                                                                                            uint64_t numberOfTuples);

uint64_t NES__Runtime__TupleBuffer__getOriginId(void* thisPtr);

void NES__Runtime__TupleBuffer__setOriginId(void* thisPtr, uint64_t value);

uint64_t NES__Runtime__TupleBuffer__getWatermark(void* thisPtr);

void NES__Runtime__TupleBuffer__setWatermark(void* thisPtr, uint64_t value);

uint64_t NES__Runtime__TupleBuffer__getCreationTimestampInMS(void* thisPtr);

void NES__Runtime__TupleBuffer__setSequenceNumber(void* thisPtr, uint64_t sequenceNumber);

uint64_t NES__Runtime__TupleBuffer__getSequenceNumber(void* thisPtr);

void NES__Runtime__TupleBuffer__setCreationTimestampInMS(void* thisPtr, uint64_t value);

}// namespace NES::Runtime::ProxyFunctions

#endif // NES_RUNTIME_INCLUDE_EXECUTION_TUPLEBUFFERPROXYFUNCTIONS_HPP_
