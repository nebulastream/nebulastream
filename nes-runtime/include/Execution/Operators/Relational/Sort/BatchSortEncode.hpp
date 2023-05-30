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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTENCODE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTENCODE_HPP_

#include <Exceptions/NotImplementedException.hpp>

#include <cfloat>
#include <cstring>
#include <climits>
#include <cstdint>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Our radix sort sorts in chunks of bytes. To perform our radix sort, we need to encode the sort column.
 * Insprired by DuckDB's radix sort implementation.
 * https://github.com/duckdb/duckdb/blob/e9f6ba553a108769f7c34a6af9354f3044e338e8/src/include/duckdb/common/radix.hpp
 */

#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))

template <typename T>
const T Load(uint8_t *ptr) {
    T ret;
    memcpy(&ret, ptr, sizeof(ret));
    return ret;
}

static inline uint8_t FlipSign(uint8_t key_byte) {
    return key_byte ^ 128;
}

template <typename T, typename R>
R encodeData(T) {
    throw Exceptions::NotImplementedException("encodeData not implemented for this type");
}

template <>
bool encodeData(bool value) {
    return (value ? 1 : 0);
}

template <>
int8_t encodeData(int8_t value) {
    auto* firstBytePtr = reinterpret_cast<uint8_t*>(&value);
    *firstBytePtr = FlipSign(*firstBytePtr);
    return value;
}

int16_t encodeData(int16_t value) {
    int16_t tmp = BSWAP16(value);
    auto* firstBytePtr = reinterpret_cast<uint8_t*>(&tmp);
    *firstBytePtr = FlipSign(*firstBytePtr);
    return tmp;
}

int32_t encodeData(int32_t value) {
    int32_t tmp = BSWAP32(value);
    auto* firstBytePtr = reinterpret_cast<uint8_t*>(&tmp);
    *firstBytePtr = FlipSign(*firstBytePtr);
    return tmp;
}

int64_t encodeData(int64_t value) {
    int64_t tmp = BSWAP64(value);
    auto* firstBytePtr = reinterpret_cast<uint8_t*>(&tmp);
    *firstBytePtr = FlipSign(*firstBytePtr);
    return tmp;
}

uint8_t encodeData(uint8_t value) {
    return value;
}

uint16_t encodeData(uint16_t value) {
    return BSWAP16(value);
}

uint32_t encodeData(uint32_t value) {
    return BSWAP32(value);
}

uint64_t encodeData(uint64_t value) {
    return BSWAP64(value);
}

uint32_t encodeData(float value) {
    uint64_t buff;
    //! zero
    if (value == 0) {
        buff = 0;
        buff |= (1u << 31);
        return buff;
    }
    //! infinity
    if (value > FLT_MAX) {
        return UINT_MAX - 1;
    }
    //! -infinity
    if (value < -FLT_MAX) {
        return 0;
    }
    buff = Load<uint64_t>((uint8_t*)&value);
    if ((buff & (1u << 31)) == 0) { //! +0 and positive numbers
        buff |= (1u << 31);
    } else {          //! negative numbers
        buff = ~buff; //! complement 1
    }

    return BSWAP32(buff);
}

uint64_t encodeData(double value) {
    uint64_t buff;
    //! zero
    if (value == 0) {
        buff = 0;
        buff += (1ull << 63);
        return buff;
    }
    //! infinity
    if (value > DBL_MAX) {
        return ULLONG_MAX - 1;
    }
    //! -infinity
    if (value < -DBL_MAX) {
        return 0;
    }
    buff = Load<uint64_t>((uint8_t*)&value);
    if (buff < (1ull << 63)) { //! +0 and positive numbers
        buff += (1ull << 63);
    } else {          //! negative numbers
        buff = ~buff; //! complement 1
    }
    return BSWAP64(buff);
}

} // namespace NES::Runtime::Execution::Operators
#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTENCODE_HPP_