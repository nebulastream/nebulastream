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

#include <SimdIntegerInputParser.hpp>

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <ErrorHandling.hpp>
#include <InputParserRegistry.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>

namespace NES
{

template <typename T>
struct SimdIntParseResult
{
    T value;
    bool isNull;
};

namespace
{
/// simdjson `numberparsing.h` SWAR constants (little-endian; both ARM and x86 are LE).
constexpr uint64_t SWAR_HI = 0xF0F0F0F0F0F0F0F0ULL;
constexpr uint64_t SWAR_ADD6 = 0x0606060606060606ULL;
constexpr uint64_t SWAR_EXP = 0x3333333333333333ULL; /// per-byte 0x33 iff the byte is an ASCII digit
constexpr uint64_t SWAR_SUB = 0x3030303030303030ULL;
constexpr uint64_t SWAR_MASK = 0x000000FF000000FFULL;
constexpr uint64_t SWAR_MUL1 = 0x000F424000000064ULL; /// 100 + (1000000 << 32)
constexpr uint64_t SWAR_MUL2 = 0x0000271000000001ULL; /// 1 + (10000 << 32)

/// simdjson is_made_of_eight_digits_fast: load 8 bytes and fold to 0x33 per byte iff every byte is '0'..'9'.
inline bool isMadeOfEightDigits(const char* chars) noexcept
{
    uint64_t val;
    std::memcpy(&val, chars, sizeof(val));
    return (((val & SWAR_HI) | (((val + SWAR_ADD6) & SWAR_HI) >> 4)) == SWAR_EXP);
}

/// simdjson parse_eight_digits_unrolled: convert 8 packed ASCII digits to their numeric value (0..99999999).
inline uint32_t parseEightDigitsUnrolled(const char* chars) noexcept
{
    uint64_t val;
    std::memcpy(&val, chars, sizeof(val));
    val -= SWAR_SUB;
    val = (val * 10) + (val >> 8);
    val = (((val & SWAR_MASK) * SWAR_MUL1) + (((val >> 16) & SWAR_MASK) * SWAR_MUL2)) >> 32;
    return static_cast<uint32_t>(val);
}

/// Parse [p, end) as a non-empty run of ASCII digits into `out`. Eight digits at a time via SWAR, then a
/// scalar tail (mirrors simdjson parse_digit). Returns false if the range is empty, contains a non-digit,
/// or the magnitude exceeds uint64 (checked per step so leading zeros stay correct). On false `out` is
/// unspecified.
inline bool parseMagnitude(const char* p, const char* end, uint64_t& out) noexcept
{
    if (p == end)
    {
        return false;
    }
    uint64_t acc = 0;
    bool ok = true;
    while (end - p >= 8)
    {
        if (not isMadeOfEightDigits(p))
        {
            break; /// the scalar tail finds and reports the offending byte
        }
        const uint64_t chunk = parseEightDigitsUnrolled(p);
        ok = ok && (acc <= (std::numeric_limits<uint64_t>::max() - chunk) / 100000000ULL);
        acc = (acc * 100000000ULL) + chunk;
        p += 8;
    }
    while (p != end)
    {
        const auto digit = static_cast<uint8_t>(static_cast<uint8_t>(*p) - static_cast<uint8_t>('0'));
        if (digit > 9)
        {
            return false;
        }
        ok = ok && (acc <= (std::numeric_limits<uint64_t>::max() - digit) / 10ULL);
        acc = (acc * 10ULL) + digit;
        ++p;
    }
    out = acc;
    return ok;
}

/// Parse the whole field [p, end) into a signed/unsigned `T`. Full-field consumption is required (a partial
/// parse like "12x" is rejected), leading zeros are accepted, and the value is range-checked against `T`.
template <typename T>
bool parseInteger(const char* p, const char* end, T& out) noexcept
{
    if (p == end)
    {
        return false;
    }
    if constexpr (std::is_signed_v<T>)
    {
        const bool negative = (*p == '-');
        const bool hasSign = negative || (*p == '+');
        p += static_cast<size_t>(hasSign);
        uint64_t magnitude = 0;
        if (not parseMagnitude(p, end, magnitude))
        {
            return false;
        }
        /// Negative numbers reach one past the positive max (two's-complement min, e.g. INT8_MIN == -128).
        const auto positiveLimit = static_cast<uint64_t>(std::numeric_limits<T>::max());
        const uint64_t limit = negative ? positiveLimit + 1ULL : positiveLimit;
        if (magnitude > limit)
        {
            return false;
        }
        const uint64_t bits = negative ? (0ULL - magnitude) : magnitude;
        out = static_cast<T>(bits);
        return true;
    }
    else
    {
        uint64_t magnitude = 0;
        if (not parseMagnitude(p, end, magnitude))
        {
            return false;
        }
        if constexpr (sizeof(T) < sizeof(uint64_t))
        {
            if (magnitude > static_cast<uint64_t>(std::numeric_limits<T>::max()))
            {
                return false;
            }
        }
        out = static_cast<T>(magnitude);
        return true;
    }
}
}

/// Non-nullable: parse the whole field with the hand-rolled simdjson scanner. A malformed or out-of-range
/// field has no null representation here, so -- like the Fast/Default non-nullable paths -- it throws.
template <typename T, bool Nullable>
requires(not Nullable)
NAUTILUS_TAGGED_INLINE(input_parse)
T parseWithSimdInt(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    const auto* begin = reinterpret_cast<const char*>(fieldAddress);
    T result;
    if (parseInteger<T>(begin, begin + fieldSize, result))
    {
        return result;
    }
    // throw CannotFormatMalformedStringValue("Error occured during simd integer parse.");
    return result; /// TMP: sentinel for the no-throw DCE experiment (well-formed data never reaches here)
}

/// Nullable: a configured null marker -> isNull; an otherwise unparseable or out-of-range field -> isNull,
/// mirroring the Fast nullable path.
template <typename T, bool Nullable>
requires Nullable
SimdIntParseResult<T>* parseWithSimdInt(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    thread_local SimdIntParseResult<T> result;
    result.isNull = false;

    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }

    const auto* begin = reinterpret_cast<const char*>(fieldAddress);
    T parsedInt;
    if (parseInteger<T>(begin, begin + fieldSize, parsedInt))
    {
        result.value = parsedInt;
    }
    else
    {
        result.isNull = true;
        result.value = T{0};
    }
    return &result;
}

VarVal SimdINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<int8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int8_t> nautilusValue = *getMemberWithOffset<int8_t>(parseResult, offsetof(SimdIntParseResult<int8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<int8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int8_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int8_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<int16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int16_t> nautilusValue
            = *getMemberWithOffset<int16_t>(parseResult, offsetof(SimdIntParseResult<int16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<int16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int16_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int16_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<int32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int32_t> nautilusValue
            = *getMemberWithOffset<int32_t>(parseResult, offsetof(SimdIntParseResult<int32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<int32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int32_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int32_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<int64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int64_t> nautilusValue
            = *getMemberWithOffset<int64_t>(parseResult, offsetof(SimdIntParseResult<int64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<int64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int64_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int64_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdUINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<uint8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint8_t> nautilusValue
            = *getMemberWithOffset<uint8_t>(parseResult, offsetof(SimdIntParseResult<uint8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<uint8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint8_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint8_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdUINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<uint16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint16_t> nautilusValue
            = *getMemberWithOffset<uint16_t>(parseResult, offsetof(SimdIntParseResult<uint16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<uint16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint16_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint16_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdUINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<uint32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint32_t> nautilusValue
            = *getMemberWithOffset<uint32_t>(parseResult, offsetof(SimdIntParseResult<uint32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<uint32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint32_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint32_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdUINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithSimdInt<uint64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint64_t> nautilusValue
            = *getMemberWithOffset<uint64_t>(parseResult, offsetof(SimdIntParseResult<uint64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(SimdIntParseResult<uint64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint64_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint64_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal SimdINT8InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<int8_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int8_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int8_t> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int8_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdINT16InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<int16_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int16_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int16_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int16_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdINT32InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<int32_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int32_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int32_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int32_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdINT64InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<int64_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int64_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int64_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<int64_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdUINT8InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<uint8_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint8_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint8_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint8_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdUINT16InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<uint16_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint16_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint16_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint16_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdUINT32InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<uint32_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint32_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint32_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint32_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal SimdUINT64InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<uint64_t> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint64_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint64_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithSimdInt<uint64_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdINT64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdUINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdUINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdUINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdUINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdUINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdUINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterSimdUINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<SimdUINT64InputParser>();
}
}
