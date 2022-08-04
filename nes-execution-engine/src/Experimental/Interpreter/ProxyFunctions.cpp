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
// #include <Runtime/TupleBuffer.hpp>
#include <cmath>
#include <cstdint>

#include <Experimental/Interpreter/ProxyFunctions.hpp>

namespace NES::Runtime::ProxyFunctions {
void* NES__Runtime__TupleBuffer__getBuffer(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getBuffer();
};
uint64_t NES__Runtime__TupleBuffer__getBufferSize(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getBufferSize();
};
uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getNumberOfTuples();
};
// void NES__Runtime__TupleBuffer__setNumberOfTuples(void* thisPtr, uint64_t numberOfTuples) {
//     auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
//     return thisPtr_->setNumberOfTuples(numberOfTuples);
// };
__attribute__((always_inline)) void NES__Runtime__TupleBuffer__setNumberOfTuples(void *thisPtr, uint64_t numberOfTuples) {
   NES::Runtime::TupleBuffer *tupleBuffer = static_cast<NES::Runtime::TupleBuffer*>(thisPtr);
   tupleBuffer->setNumberOfTuples(numberOfTuples);
}
uint64_t NES__Runtime__TupleBuffer__getWatermark(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getWatermark();
};
void NES__Runtime__TupleBuffer__setWatermark(void* thisPtr, uint64_t value) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->setWatermark(value);
};
uint64_t NES__Runtime__TupleBuffer__getCreationTimestamp(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getCreationTimestamp();
};
void NES__Runtime__TupleBuffer__setSequenceNumber(void* thisPtr, uint64_t sequenceNumber) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->setSequenceNumber(sequenceNumber);
};
uint64_t NES__Runtime__TupleBuffer__getSequenceNumber(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getSequenceNumber();
}
void NES__Runtime__TupleBuffer__setCreationTimestamp(void* thisPtr, uint64_t value) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->setCreationTimestamp(value);
}

__attribute__((always_inline)) void printInt64(int64_t inputValue) {
    printf("I64 Value: %ld\n", inputValue);
}


// Playground

__attribute__((always_inline)) uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}
inline uint32_t rotl32(uint32_t x, int8_t r) { 
    return (x << r) | (x >> (32 - r)); 
}

inline uint32_t murmurHashKey(uint64_t k) {
        // inline hash_t hashKey(uint64_t k, hash_t seed) const {
        uint32_t h1 = 0;
        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        uint32_t k1 = k;
        uint32_t k2 = k >> 32;

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        k2 *= c1;
        k1 = rotl32(k1, 15);
        k2 *= c2;

        h1 ^= k2;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        return fmix32(h1);
}

#if defined(__SSE__)
#include <x86intrin.h>
// inline uint64_t crc32HashKey(uint64_t k, uint64_t seed) {
        // uint64_t result1 = _mm_crc32_u64(seed, k);
        // uint64_t result2 = _mm_crc32_u64(0x04c11db7, k);
        // return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
        // return k + seed;
// }

__attribute__((__always_inline__, __nodebug__, __target__("sse4.2")))int64_t getCRC32Hash(uint64_t inputValue, uint64_t seed) {
    // uint32_t hashKeyValue = crc32HashKey(inputValue, 0);
    uint64_t result1 = _mm_crc32_u64(seed, inputValue);
    uint64_t result2 = _mm_crc32_u64(0x04c11db7, inputValue);
    return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
    // printf("Current input value: %ld\n", inputValue);
    // printf("Current input value: %u\n", hashKeyValue);
    // return (int64_t)hashKeyValue;
    return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
}
# else 
inline auto crc32HashKey(uint64_t k, uint64_t seed) {
    std::cout << "DUMMY CRC32 FUNCTION\n";
    return k + seed;
}
#endif//__SSE__

__attribute__((always_inline)) int64_t getMurMurHash(uint64_t inputValue) {
    uint32_t hashKeyValue = murmurHashKey(inputValue);
    return (int64_t)hashKeyValue;
}

__attribute__((always_inline)) double standardDeviationGetMean(int64_t size, void* bufferPtr) {
    int64_t *buffer = (int64_t *) bufferPtr;
    int64_t runningSum = 0;
    for(int64_t i = 0; i < size; ++i) {
        runningSum += buffer[i];
    }
    // printf("Size: %ld\n", buffer[0]);
    // printf("Size: %ld\n", size);
    return runningSum / (double)size;
    // return 25.5277;
    // return (double)runningSum / (double)size;
}
__attribute__((always_inline)) double standardDeviationGetVariance(double runningDeviationSum, double mean, int64_t inputValue) {
    return runningDeviationSum += (inputValue - mean) * (inputValue - mean);
}
__attribute__((always_inline)) double standardDeviationGetStdDev(double deviationSum, int64_t size) {
    return std::sqrt(deviationSum) / size;
}

__attribute__((always_inline)) char customToUpper(char c) {
    if(c>=97 && c<=122)
    {
        return c-32;
    }
    else
    {
        return c;
    }
}

__attribute__((always_inline)) void stringToUpperCase(int64_t i, void *inputString) {
    auto test = (char**) inputString + i;
    char *currentString = *test;
    for(char c = *currentString; c; c=*++currentString) {
//        *currentString = ::toupper(c);
        *currentString = customToUpper(c);
    };
}

// __attribute__((always_inline)) int64_t stringToUpperCasePlusCount(int64_t val1, int64_t i, void *inputString) {
//     auto test = (char**) inputString + i;
//     char *currentString = *test;
//     int numCharactersProcessed = 0;
//     for(char c = *currentString; c; c=*++currentString) {
//         *currentString = ::toupper(c);
//         ++numCharactersProcessed;
//     };
//     return val1 + numCharactersProcessed;
// }



}// namespace NES::Runtime::ProxyFunctions