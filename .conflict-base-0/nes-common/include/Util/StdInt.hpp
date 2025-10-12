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

#pragma once
#include <cstdint>

/**
 * @brief Implements unsigned user-defined literals that return the corresponding uintX_t type
 */
consteval uint8_t operator""_u8(unsigned long long value)
{
    return static_cast<uint8_t>(value);
}

consteval uint16_t operator""_u16(unsigned long long value)
{
    return static_cast<uint16_t>(value);
}

consteval uint32_t operator""_u32(unsigned long long value)
{
    return static_cast<uint32_t>(value);
}

consteval uint64_t operator""_u64(unsigned long long value)
{
    return static_cast<uint64_t>(value);
}

/**
 * @brief We require this helper struct, as there is no such thing as a negative integer literal (https://stackoverflow.com/a/23430371)
 * We overload the operator- and the implicit conversion operator to be able to use it seamlessly, e.g. CustomClass(-2_s8), or CustomClass(42_s64)
 */
template <typename T>
struct HelperStructLiterals
{
    T val;

    [[maybe_unused]] constexpr inline explicit HelperStructLiterals(T v) : val(v) { }

    constexpr inline T operator-() const { return -val; }

    constexpr inline T operator+() const { return +val; }

    constexpr inline operator T() const { return val; }
};

/**
 * @brief We have to return here our own helper struct as otherwise, we can not parse negative constants.
 */
consteval HelperStructLiterals<int8_t> operator""_s8(unsigned long long value)
{
    return HelperStructLiterals<int8_t>(value);
}

consteval HelperStructLiterals<int16_t> operator""_s16(unsigned long long value)
{
    return HelperStructLiterals<int16_t>(value);
}

consteval HelperStructLiterals<int32_t> operator""_s32(unsigned long long value)
{
    return HelperStructLiterals<int32_t>(value);
}

consteval HelperStructLiterals<int64_t> operator""_s64(unsigned long long value)
{
    return HelperStructLiterals<int64_t>(value);
}
