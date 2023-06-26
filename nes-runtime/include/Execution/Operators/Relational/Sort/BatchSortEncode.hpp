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

#include <bit>
#include <cfloat>
#include <climits>
#include <cstdint>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Our radix sort sorts in chunks of bytes. To perform our radix sort, we need to encode the sort column.
 * Inspired by DuckDB's radix sort implementation.
 * https://github.com/duckdb/duckdb/blob/e9f6ba553a108769f7c34a6af9354f3044e338e8/src/include/duckdb/common/radix.hpp
 */

/**
 * @brief Flip the sign bit of a signed integer value.
 * @param value
 * @return uint8_t
 */
inline constexpr uint8_t FlipSign(uint8_t keyByte) { return keyByte ^ 128; }

/**
 * @brief Byte swap a value
 * @param value
 * @return uint8_t
 */
template<typename T>
inline constexpr T byteSwap(T value) {
    static_assert(std::is_integral<T>::value, "byteSwap can only be used with integral types.");
    switch (sizeof(T)) {
        case 1: return value;
        case 2: return std::endian::big == std::endian::native ? value : __builtin_bswap16(value);
        case 4: return std::endian::big == std::endian::native ? value : __builtin_bswap32(value);
        case 8: return std::endian::big == std::endian::native ? value : __builtin_bswap64(value);
        default:
            throw Exceptions::NotImplementedException("byteSwap only supports integral types with sizes 1, 2, 4, or 8 bytes.");
    }
}

/**
 * @brief Traits class for encoding values for sorting
 * Trait classes are discussed in detail in e.g.:
 * https://accu.org/journals/overload/9/43/frogley_442/
 * @tparam T
 * @tparam void
 */
template<typename T, typename = void>
struct EncoderTraits {
    using EncodedType = T;

    static EncodedType Encode(T, bool) { throw Exceptions::NotImplementedException("Encode not implemented for this type"); }
};

/**
 * @brief Encode bool values for sorting
 * @tparam T
 */
template<>
struct EncoderTraits<bool> {
    using EncodedType = bool;

    static EncodedType Encode(bool value, bool isDescending) {
        if (isDescending)
            value = !value;
        return value;
    }
};

/**
 * @brief Encode signed integral values for sorting
 * @tparam T
 */
template<typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_integral<T>::value && std::is_signed<T>::value>::type> {
    using EncodedType = T;

    static EncodedType Encode(T value, bool descending) {
        value = byteSwap(value);
        auto* firstBytePtr = reinterpret_cast<uint8_t*>(&value);
        *firstBytePtr = FlipSign(*firstBytePtr);
        if (descending)
            value = ~value;
        return value;
    }
};

/**
 * @brief Encode unsigned integral values for sorting
 * @tparam T
 */
template<typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_integral<T>::value && std::is_unsigned<T>::value>::type> {
    using EncodedType = T;

    static EncodedType Encode(T value, bool descending) {
        value = byteSwap(value);
        if (descending)
            value = ~value;
        return value;
    }
};

/**
 * @brief Encode floating point values for sorting
 * @tparam T
 */
template<typename T>
struct EncoderTraits<T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
    // float and double are encoded as uint32_t and uint64_t respectively
    using EncodedType = typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type;

    static EncodedType Encode(T value, bool descending) {
        EncodedType encodedValue;
        if (value == 0) {
            encodedValue = 0;
            encodedValue |= (1ull << (sizeof(EncodedType) * 8 - 1));
            return encodedValue;
        }
        if (value > std::numeric_limits<T>::max()) {
            return std::numeric_limits<EncodedType>::max() - 1;
        }
        if (value < -std::numeric_limits<T>::max()) {
            return 0;
        }
        encodedValue = *reinterpret_cast<EncodedType*>(&value);
        if (encodedValue < (1ull << (sizeof(EncodedType) * 8 - 1))) {
            encodedValue += (1ull << (sizeof(EncodedType) * 8 - 1));
        } else {
            encodedValue = ~encodedValue;
        }
        if (descending)
            encodedValue = ~encodedValue;
        return byteSwap(encodedValue);
    }
};

template<typename T>
typename EncoderTraits<T>::EncodedType encodeData(T value, bool descending = false) {
    return EncoderTraits<T>::Encode(value, descending);
}

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHSORTENCODE_HPP_