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

/**
 * The function RadixSortMSD is taken from DuckDB (MIT license):
 * MIT License
 *
 * Copyright 2018-2023 Stichting DuckDB Foundation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_BATCHSORTENCODE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_BATCHSORTENCODE_HPP_

#include <Exceptions/NotImplementedException.hpp>

#include <bit>
#include <cfloat>
#include <climits>
#include <cstdint>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Our radix sort sorts in chunks of bytes. To perform our radix sort, we need to encode the sort column.
 * Encoding is taken from DuckDB with modification (C++11 features and adaption to Nautilus)
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
        if (isDescending) {
            value = !value;
        }
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
        if (descending) {
            value = ~value;
        }
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
        if (descending) {
            value = ~value;
        }
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
        } else if (value > std::numeric_limits<T>::max()) {
            encodedValue = std::numeric_limits<EncodedType>::max() - 1;
        } else if (value < -std::numeric_limits<T>::max()) {
            encodedValue = 0;
        } else {
            encodedValue = *reinterpret_cast<EncodedType*>(&value);
            if (encodedValue < (1ull << (sizeof(EncodedType) * 8 - 1))) {
                encodedValue += (1ull << (sizeof(EncodedType) * 8 - 1));
            } else {
                encodedValue = ~encodedValue;
            }
            if (descending) {
                encodedValue = ~encodedValue;
            }
            encodedValue = byteSwap(encodedValue);
        }
        return encodedValue;
    }
};

template<typename T>
typename EncoderTraits<T>::EncodedType encodeData(T value, bool descending = false) {
    return EncoderTraits<T>::Encode(value, descending);
}

}// namespace NES::Runtime::Execution::Operators
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORT_BATCHSORTENCODE_HPP_
