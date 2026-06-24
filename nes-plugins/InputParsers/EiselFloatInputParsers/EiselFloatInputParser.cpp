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

#include <EiselFloatInputParser.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <ErrorHandling.hpp>
#include <InputParserRegistry.hpp>
#include <PowersOfFive.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>

namespace NES
{

template <typename T>
struct EiselFloatParseResult
{
    T value;
    bool isNull;
};

namespace
{
/// Per-format IEEE-754 constants, mirroring fast_float's `binary_format<T>` (see float_common.h). One table
/// of 128-bit powers of five (PowersOfFive.hpp) serves both formats; it spans double's wider exponent range.
template <typename T>
struct BinaryFormat;

template <>
struct BinaryFormat<double>
{
    using Bits = uint64_t;
    static constexpr int MANTISSA_EXPLICIT_BITS = 52;
    static constexpr int MINIMUM_EXPONENT = -1023;
    static constexpr int INFINITE_POWER = 0x7FF;
    static constexpr int SIGN_INDEX = 63;
    static constexpr int SMALLEST_POWER_OF_TEN = -342;
    static constexpr int LARGEST_POWER_OF_TEN = 308;
    static constexpr int MIN_EXPONENT_FAST_PATH = -22;
    static constexpr int MAX_EXPONENT_FAST_PATH = 22;
    static constexpr int MIN_EXPONENT_ROUND_TO_EVEN = -4;
    static constexpr int MAX_EXPONENT_ROUND_TO_EVEN = 23;
    static constexpr uint64_t MAX_MANTISSA_FAST_PATH = uint64_t(2) << MANTISSA_EXPLICIT_BITS;
    static constexpr double EXACT_POWER_OF_TEN[]
        = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
};

template <>
struct BinaryFormat<float>
{
    using Bits = uint32_t;
    static constexpr int MANTISSA_EXPLICIT_BITS = 23;
    static constexpr int MINIMUM_EXPONENT = -127;
    static constexpr int INFINITE_POWER = 0xFF;
    static constexpr int SIGN_INDEX = 31;
    static constexpr int SMALLEST_POWER_OF_TEN = -64;
    static constexpr int LARGEST_POWER_OF_TEN = 38;
    static constexpr int MIN_EXPONENT_FAST_PATH = -10;
    static constexpr int MAX_EXPONENT_FAST_PATH = 10;
    static constexpr int MIN_EXPONENT_ROUND_TO_EVEN = -17;
    static constexpr int MAX_EXPONENT_ROUND_TO_EVEN = 10;
    static constexpr uint64_t MAX_MANTISSA_FAST_PATH = uint64_t(2) << MANTISSA_EXPLICIT_BITS;
    static constexpr float EXACT_POWER_OF_TEN[] = {1e0F, 1e1F, 1e2F, 1e3F, 1e4F, 1e5F, 1e6F, 1e7F, 1e8F, 1e9F, 1e10F};
};

/// fast_float/simdjson SWAR constants used to scan eight ASCII digits at once.
constexpr uint64_t SWAR_HI = 0xF0F0F0F0F0F0F0F0ULL;
constexpr uint64_t SWAR_ADD6 = 0x0606060606060606ULL;
constexpr uint64_t SWAR_EXP = 0x3333333333333333ULL;
constexpr uint64_t SWAR_SUB = 0x3030303030303030ULL;
constexpr uint64_t SWAR_MASK = 0x000000FF000000FFULL;
constexpr uint64_t SWAR_MUL1 = 0x000F424000000064ULL;
constexpr uint64_t SWAR_MUL2 = 0x0000271000000001ULL;

inline bool isDigit(char c) noexcept
{
    return static_cast<uint8_t>(c) >= static_cast<uint8_t>('0') && static_cast<uint8_t>(c) <= static_cast<uint8_t>('9');
}

inline bool isMadeOfEightDigits(const char* chars) noexcept
{
    uint64_t val;
    std::memcpy(&val, chars, sizeof(val));
    return (((val & SWAR_HI) | (((val + SWAR_ADD6) & SWAR_HI) >> 4)) == SWAR_EXP);
}

inline uint32_t parseEightDigitsUnrolled(const char* chars) noexcept
{
    uint64_t val;
    std::memcpy(&val, chars, sizeof(val));
    val -= SWAR_SUB;
    val = (val * 10) + (val >> 8);
    val = (((val & SWAR_MASK) * SWAR_MUL1) + (((val >> 16) & SWAR_MASK) * SWAR_MUL2)) >> 32;
    return static_cast<uint32_t>(val);
}

/// fast_float loop_parse_if_eight_digits: fold whole eight-digit groups into the mantissa (the accumulator
/// may overflow on very long inputs; that is corrected by the too-many-digits recompute in parseNumberString).
inline void loopParseIfEightDigits(const char*& p, const char* end, uint64_t& i) noexcept
{
    while ((end - p) >= 8 && isMadeOfEightDigits(p))
    {
        i = (i * 100000000ULL) + parseEightDigitsUnrolled(p);
        p += 8;
    }
}

struct ParsedNumber
{
    uint64_t mantissa;
    int64_t exponent;
    bool negative;
    bool tooManyDigits;
};

/// Port of fast_float `parse_number_string` (ascii_number.h): decompose [p, end) into a 64-bit mantissa, a
/// base-10 exponent and a sign. A leading '+' is accepted (CSV-friendly). The whole field must be consumed,
/// so trailing junk like "1.2x" is rejected; `inf`/`nan`/hex are not recognised and fail here.
inline bool parseNumberString(const char* p, const char* end, ParsedNumber& out) noexcept
{
    if (p == end)
    {
        return false;
    }
    const bool negative = (*p == '-');
    if (*p == '-' || *p == '+')
    {
        ++p;
        if (p == end)
        {
            return false;
        }
    }

    const char* const startDigits = p;
    uint64_t i = 0;
    loopParseIfEightDigits(p, end, i);
    while (p != end && isDigit(*p))
    {
        i = (10 * i) + static_cast<uint64_t>(*p - '0');
        ++p;
    }
    const char* const endIntegerPart = p;
    int64_t digitCount = static_cast<int64_t>(endIntegerPart - startDigits);

    int64_t exponent = 0;
    const char* fractionStart = nullptr;
    if (p != end && *p == '.')
    {
        ++p;
        const char* const before = p;
        fractionStart = before;
        loopParseIfEightDigits(p, end, i);
        while (p != end && isDigit(*p))
        {
            i = (10 * i) + static_cast<uint64_t>(*p - '0');
            ++p;
        }
        exponent = before - p; /// negative: number of fractional digits
        digitCount -= exponent;
    }
    if (digitCount == 0)
    {
        return false; /// no digits at all
    }

    int64_t explicitExponent = 0;
    if (p != end && (*p == 'e' || *p == 'E'))
    {
        ++p;
        bool negativeExponent = false;
        if (p != end && *p == '-')
        {
            negativeExponent = true;
            ++p;
        }
        else if (p != end && *p == '+')
        {
            ++p;
        }
        if (p == end || not isDigit(*p))
        {
            return false; /// 'e' without an exponent
        }
        while (p != end && isDigit(*p))
        {
            const auto digit = static_cast<uint8_t>(*p - '0');
            if (explicitExponent < 0x10000000)
            {
                explicitExponent = (10 * explicitExponent) + digit;
            }
            ++p;
        }
        if (negativeExponent)
        {
            explicitExponent = -explicitExponent;
        }
        exponent += explicitExponent;
    }

    if (p != end)
    {
        return false; /// trailing junk -> reject (full-field consumption)
    }

    bool tooManyDigits = false;
    if (digitCount > 19)
    {
        /// Strip leading zeros (and the dot), which do not count as significant digits.
        const char* start = startDigits;
        while (start != end && (*start == '0' || *start == '.'))
        {
            if (*start == '0')
            {
                --digitCount;
            }
            ++start;
        }
        if (digitCount > 19)
        {
            tooManyDigits = true;
            /// Recompute a truncated 19-significant-digit mantissa, adjusting the exponent (fast_float).
            i = 0;
            const char* q = startDigits;
            constexpr uint64_t MINIMAL_NINETEEN_DIGIT_INTEGER = 1000000000000000000ULL;
            while (i < MINIMAL_NINETEEN_DIGIT_INTEGER && q != endIntegerPart)
            {
                i = (10 * i) + static_cast<uint64_t>(*q - '0');
                ++q;
            }
            if (i >= MINIMAL_NINETEEN_DIGIT_INTEGER)
            {
                exponent = (endIntegerPart - q) + explicitExponent;
            }
            else
            {
                q = fractionStart;
                while (i < MINIMAL_NINETEEN_DIGIT_INTEGER && q != end && isDigit(*q))
                {
                    i = (10 * i) + static_cast<uint64_t>(*q - '0');
                    ++q;
                }
                exponent = (fractionStart - q) + explicitExponent;
            }
        }
    }

    out.mantissa = i;
    out.exponent = exponent;
    out.negative = negative;
    out.tooManyDigits = tooManyDigits;
    return true;
}

struct AdjustedMantissa
{
    uint64_t mantissa;
    int32_t power2;
};

struct Value128
{
    uint64_t low;
    uint64_t high;
};

inline Value128 fullMultiplication(uint64_t a, uint64_t b) noexcept
{
#ifdef __SIZEOF_INT128__
    const auto product = static_cast<__uint128_t>(a) * b;
    return Value128{static_cast<uint64_t>(product), static_cast<uint64_t>(product >> 64)};
#else
    const uint64_t ad = static_cast<uint64_t>(static_cast<uint32_t>(a >> 32)) * static_cast<uint32_t>(b);
    const uint64_t bd = static_cast<uint64_t>(static_cast<uint32_t>(a)) * static_cast<uint32_t>(b);
    const uint64_t adbc = ad + (static_cast<uint64_t>(static_cast<uint32_t>(a)) * static_cast<uint32_t>(b >> 32));
    const uint64_t adbcCarry = static_cast<uint64_t>(adbc < ad);
    const uint64_t lo = bd + (adbc << 32);
    const uint64_t hi = (static_cast<uint64_t>(static_cast<uint32_t>(a >> 32)) * static_cast<uint32_t>(b >> 32)) + (adbc >> 32)
        + (adbcCarry << 32) + static_cast<uint64_t>(lo < bd);
    return Value128{lo, hi};
#endif
}

inline int leadingZeroes(uint64_t value) noexcept
{
    return __builtin_clzll(value);
}

/// fast_float detail::power: floor(q * log2(5)) + q + 63 for q in (-400, 350).
inline int32_t powerOfTwoFromTen(int32_t q) noexcept
{
    return (((152170 + 65536) * q) >> 16) + 63;
}

/// fast_float compute_product_approximation: approximate w * 5^q to 128 bits via the vendored table.
template <int bitPrecision>
inline Value128 computeProductApproximation(int64_t q, uint64_t w) noexcept
{
    const int index = 2 * static_cast<int>(q - EiselFloat::SMALLEST_POWER_OF_FIVE);
    Value128 firstProduct = fullMultiplication(w, EiselFloat::POWER_OF_FIVE_128[index]);
    constexpr uint64_t PRECISION_MASK = (bitPrecision < 64) ? (uint64_t{0xFFFFFFFFFFFFFFFF} >> bitPrecision) : uint64_t{0xFFFFFFFFFFFFFFFF};
    if ((firstProduct.high & PRECISION_MASK) == PRECISION_MASK)
    {
        const Value128 secondProduct = fullMultiplication(w, EiselFloat::POWER_OF_FIVE_128[index + 1]);
        firstProduct.low += secondProduct.high;
        if (secondProduct.high > firstProduct.low)
        {
            firstProduct.high++;
        }
    }
    return firstProduct;
}

/// fast_float compute_float ("Fast Number Parsing Without Fallback", Mushtak & Lemire): turn w * 10^q into a
/// correctly-rounded IEEE-754 (mantissa, power2). For w with <= 19 significant digits the product is always
/// sufficient; for more, the caller resolves ambiguity by recomputing with w+1.
template <typename T>
AdjustedMantissa computeFloat(int64_t q, uint64_t w) noexcept
{
    using BF = BinaryFormat<T>;
    AdjustedMantissa answer{};
    if (w == 0 || q < BF::SMALLEST_POWER_OF_TEN)
    {
        answer.power2 = 0;
        answer.mantissa = 0;
        return answer; /// underflow to zero
    }
    if (q > BF::LARGEST_POWER_OF_TEN)
    {
        answer.power2 = BF::INFINITE_POWER;
        answer.mantissa = 0;
        return answer; /// overflow to infinity
    }
    const int lz = leadingZeroes(w);
    w <<= lz;
    const Value128 product = computeProductApproximation<BF::MANTISSA_EXPLICIT_BITS + 3>(q, w);
    const int upperbit = static_cast<int>(product.high >> 63);
    const int shift = upperbit + 64 - BF::MANTISSA_EXPLICIT_BITS - 3;
    answer.mantissa = product.high >> shift;
    answer.power2 = static_cast<int32_t>(powerOfTwoFromTen(static_cast<int32_t>(q)) + upperbit - lz - BF::MINIMUM_EXPONENT);
    if (answer.power2 <= 0)
    {
        /// subnormal
        if (-answer.power2 + 1 >= 64)
        {
            answer.power2 = 0;
            answer.mantissa = 0;
            return answer;
        }
        answer.mantissa >>= -answer.power2 + 1;
        answer.mantissa += (answer.mantissa & 1);
        answer.mantissa >>= 1;
        answer.power2 = (answer.mantissa < (uint64_t{1} << BF::MANTISSA_EXPLICIT_BITS)) ? 0 : 1;
        return answer;
    }
    if (product.low <= 1 && q >= BF::MIN_EXPONENT_ROUND_TO_EVEN && q <= BF::MAX_EXPONENT_ROUND_TO_EVEN && (answer.mantissa & 3) == 1)
    {
        if ((answer.mantissa << shift) == product.high)
        {
            answer.mantissa &= ~uint64_t{1}; /// do not round up when exactly halfway
        }
    }
    answer.mantissa += (answer.mantissa & 1);
    answer.mantissa >>= 1;
    if (answer.mantissa >= (uint64_t{2} << BF::MANTISSA_EXPLICIT_BITS))
    {
        answer.mantissa = (uint64_t{1} << BF::MANTISSA_EXPLICIT_BITS);
        answer.power2++;
    }
    answer.mantissa &= ~(uint64_t{1} << BF::MANTISSA_EXPLICIT_BITS);
    if (answer.power2 >= BF::INFINITE_POWER)
    {
        answer.power2 = BF::INFINITE_POWER;
        answer.mantissa = 0;
    }
    return answer;
}

/// fast_float to_float: pack (sign, power2, mantissa) into the IEEE-754 bit pattern.
template <typename T>
T toFloat(bool negative, AdjustedMantissa am) noexcept
{
    using Bits = typename BinaryFormat<T>::Bits;
    Bits word = static_cast<Bits>(am.mantissa);
    word |= static_cast<Bits>(static_cast<uint64_t>(am.power2)) << BinaryFormat<T>::MANTISSA_EXPLICIT_BITS;
    word |= static_cast<Bits>(negative ? 1 : 0) << BinaryFormat<T>::SIGN_INDEX;
    T value;
    std::memcpy(&value, &word, sizeof(T));
    return value;
}

/// Clinger fast path: for a small exactly-representable mantissa and exponent, a single exact multiply/divide
/// by a power of ten is already correctly rounded.
template <typename T>
bool tryFastPath(const ParsedNumber& parsed, T& out) noexcept
{
    using BF = BinaryFormat<T>;
    if (parsed.tooManyDigits || parsed.exponent < BF::MIN_EXPONENT_FAST_PATH || parsed.exponent > BF::MAX_EXPONENT_FAST_PATH
        || parsed.mantissa > BF::MAX_MANTISSA_FAST_PATH)
    {
        return false;
    }
    T value = static_cast<T>(parsed.mantissa);
    if (parsed.exponent < 0)
    {
        value = value / BF::EXACT_POWER_OF_TEN[-parsed.exponent];
    }
    else
    {
        value = value * BF::EXACT_POWER_OF_TEN[parsed.exponent];
    }
    out = parsed.negative ? -value : value;
    return true;
}

/// Parse the whole field [p, end) into a float/double. Returns false (malformed) on a structural error or on
/// a >19-significant-digit input whose rounding the lean Lemire step cannot resolve without a decimal fallback.
template <typename T>
bool parseFloat(const char* p, const char* end, T& out) noexcept
{
    ParsedNumber parsed{};
    if (not parseNumberString(p, end, parsed))
    {
        return false;
    }
    if (tryFastPath<T>(parsed, out))
    {
        return true;
    }
    const AdjustedMantissa am = computeFloat<T>(parsed.exponent, parsed.mantissa);
    if (parsed.tooManyDigits)
    {
        const AdjustedMantissa amUp = computeFloat<T>(parsed.exponent, parsed.mantissa + 1);
        if (am.mantissa != amUp.mantissa || am.power2 != amUp.power2)
        {
            return false; /// ambiguous truncation; the decimal slow path is intentionally omitted
        }
    }
    out = toFloat<T>(parsed.negative, am);
    return true;
}
}

/// Non-nullable: a malformed field has no null representation, so -- like the Fast/Default non-nullable paths
/// -- it throws.
template <typename T, bool Nullable>
requires(not Nullable)
NAUTILUS_TAGGED_INLINE(input_parse)
T parseWithEiselFloat(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    const auto* begin = reinterpret_cast<const char*>(fieldAddress);
    T result;
    if (parseFloat<T>(begin, begin + fieldSize, result))
    {
        return result;
    }
    throw CannotFormatMalformedStringValue("Error occured during eisel float parse.");
}

/// Nullable: a configured null marker -> isNull; an otherwise unparseable field -> isNull, mirroring the Fast
/// nullable path.
template <typename T, bool Nullable>
requires Nullable
EiselFloatParseResult<T>*
parseWithEiselFloat(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    thread_local EiselFloatParseResult<T> result;
    result.isNull = false;

    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }

    const auto* begin = reinterpret_cast<const char*>(fieldAddress);
    T parsedFloat;
    if (parseFloat<T>(begin, begin + fieldSize, parsedFloat))
    {
        result.value = parsedFloat;
    }
    else
    {
        result.isNull = true;
        result.value = T{0};
    }
    return &result;
}

VarVal EiselFloatF64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithEiselFloat<double, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<double> nautilusValue
            = *getMemberWithOffset<double>(parseResult, offsetof(EiselFloatParseResult<double>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(EiselFloatParseResult<double>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<double> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithEiselFloat<double, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal EiselFloatF32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithEiselFloat<float, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<float> nautilusValue = *getMemberWithOffset<float>(parseResult, offsetof(EiselFloatParseResult<float>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(EiselFloatParseResult<float>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<float> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithEiselFloat<float, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal EiselFloatF32InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<float> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithEiselFloat<float, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<float> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithEiselFloat<float, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal EiselFloatF64InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<double> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithEiselFloat<double, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<double> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithEiselFloat<double, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterEiselFloatF32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<EiselFloatF32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterEiselFloatF64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<EiselFloatF64InputParser>();
}
}
