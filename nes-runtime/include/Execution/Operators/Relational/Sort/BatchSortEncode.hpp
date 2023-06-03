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
#include <bit>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Our radix sort sorts in chunks of bytes. To perform our radix sort, we need to encode the sort column.
 * Inspired by DuckDB's radix sort implementation.
 * https://github.com/duckdb/duckdb/blob/e9f6ba553a108769f7c34a6af9354f3044e338e8/src/include/duckdb/common/radix.hpp
 */

inline constexpr uint8_t FlipSign(uint8_t key_byte) {
    return key_byte ^ 128;
}

// Utility functions for byte swapping
template <typename T>
inline constexpr T BSWAP(T value) {
    static_assert(std::is_integral<T>::value, "BSWAP can only be used with integral types.");
    if (sizeof(T) == 1) {
        return value;
    } else if (sizeof(T) == 2) {
        return std::endian::big == std::endian::native ? value : __builtin_bswap16(value);
    } else if (sizeof(T) == 4) {
        return std::endian::big == std::endian::native ? value : __builtin_bswap32(value);
    } else if (sizeof(T) == 8) {
        return std::endian::big == std::endian::native ? value : __builtin_bswap64(value);

    } else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                      "BSWAP only supports integral types with sizes 1, 2, 4, or 8 bytes.");
    }
}

template <typename T, typename = void>
struct EncoderTraits {
    using EncodedType = T;

    static EncodedType Encode(T) {
        throw Exceptions::NotImplementedException("Encode not implemented for this type");
    }
};

template <>
struct EncoderTraits<bool> {
    using EncodedType = bool;

    static EncodedType Encode(bool value) {
        return value;
    }
};

template <typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_same<T, bool>::value>::type> {
    using EncodedType = bool;

    static EncodedType Encode(bool value) {
        return value;
    }
};

template <typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_integral<T>::value && std::is_signed<T>::value>::type> {
    using EncodedType = T;

    static EncodedType Encode(T value) {
        value = BSWAP(value);
        auto* firstBytePtr = reinterpret_cast<uint8_t*>(&value);
        *firstBytePtr = FlipSign(*firstBytePtr);
        return value;
    }
};

template <typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_integral<T>::value && std::is_unsigned<T>::value>::type> {
    using EncodedType = T;

    static EncodedType Encode(T value) {
        return BSWAP(value);
    }
};

template <typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
    using EncodedType = typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type;

    static EncodedType Encode(T value) {
        using UnsignedType = typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type;
        UnsignedType encodedValue;
        if (value == 0) {
            encodedValue = 0;
            encodedValue |= (1ull << (sizeof(UnsignedType) * 8 - 1));
            return encodedValue;
        }
        if (value > std::numeric_limits<T>::max()) {
            return std::numeric_limits<UnsignedType>::max() - 1;
        }
        if (value < -std::numeric_limits<T>::max()) {
            return 0;
        }
        encodedValue = *reinterpret_cast<UnsignedType*>(&value);
        if (encodedValue < (1ull << (sizeof(UnsignedType) * 8 - 1))) {
            encodedValue += (1ull << (sizeof(UnsignedType) * 8 - 1));
        } else {
            encodedValue = ~encodedValue;
        }
        return BSWAP(encodedValue);
    }
};

template <typename T>
typename EncoderTraits<T>::EncodedType encodeData(T value) {
    return EncoderTraits<T>::Encode(value);
}

} // namespace NES::Runtime::Execution::Operators
#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTENCODE_HPP_