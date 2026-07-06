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

#include <TracedFloatInputParser.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <fast_float/fast_float.h>
#include <ErrorHandling.hpp>
#include <InputParserRegistry.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Tiny power-of-ten tables for the Clinger fast path (same values as fast_float's EXACT_POWER_OF_TEN /
/// the Eisel parser's table). Namespace-scope so the arrays have a stable runtime address the trace can
/// capture as a constant pointer. double covers 10^0..10^22, float 10^0..10^10.
const double POW10_F64[]
    = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
const float POW10_F32[] = {1e0F, 1e1F, 1e2F, 1e3F, 1e4F, 1e5F, 1e6F, 1e7F, 1e8F, 1e9F, 1e10F};

template <typename T>
struct FastTraits;

template <>
struct FastTraits<double>
{
    /// 10^15 < 2^53, so a <=15-digit mantissa is exactly representable -> Clinger fast path is exact.
    static constexpr uint64_t MAX_FAST_DIGITS = 15;
    static constexpr uint64_t MAX_FRAC = 22;

    static const double* pow10() noexcept { return POW10_F64; }
};

template <>
struct FastTraits<float>
{
    /// 10^7 < 2^24, exactly representable in float.
    static constexpr uint64_t MAX_FAST_DIGITS = 7;
    static constexpr uint64_t MAX_FRAC = 10;

    static const float* pow10() noexcept { return POW10_F32; }
};

template <typename T>
struct TracedFloatParseResult
{
    T value;
    bool isNull;
};

/// Non-nullable fallback for anything the traced fast path flags as `hard` (exponent notation, > 15/7
/// digits, malformed): fast_float is the same Eisel-Lemire algorithm class as the Eisel parser, so the
/// result matches. A malformed non-nullable field throws (like the Fast/Eisel non-nullable paths).
template <typename T>
NAUTILUS_TAGGED_INLINE(input_parse)
T parseFloatFallback(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    const char* p = reinterpret_cast<const char*>(fieldAddress);
    T result;
    if (auto [ptr, ec] = fast_float::from_chars(p, p + fieldSize, result); ec == std::errc())
    {
        return result;
    }
    throw CannotFormatMalformedStringValue("Error occured during traced float fallback parse.");
}

/// Nullable path: a configured null marker -> isNull; an otherwise unparseable field -> isNull (mirrors
/// the Fast/Eisel nullable path). Entirely a proxy -- the traced fast path is used only for non-nullable.
template <typename T>
TracedFloatParseResult<T>*
parseFloatNullable(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    thread_local TracedFloatParseResult<T> result;
    result.isNull = false;
    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }
    const char* p = reinterpret_cast<const char*>(fieldAddress);
    T parsed;
    if (auto [ptr, ec] = fast_float::from_chars(p, p + fieldSize, parsed); ec == std::errc())
    {
        result.value = parsed;
    }
    else
    {
        result.isNull = true;
        result.value = T{0};
    }
    return &result;
}

/// The TRACED fast path (non-nullable). Emits nautilus ops that scan the field once -- accumulate a u64
/// mantissa, count fractional digits, and set `hard` for any byte the exact fast path can't handle -- then
/// either fall back to the proxy (hard) or Clinger-scale `mantissa / 10^frac` (fused into the JIT loop).
template <typename T>
nautilus::val<T> tracedFastParse(const nautilus::val<int8_t*>& addr, const nautilus::val<uint64_t>& size)
{
    using Tr = FastTraits<T>;
    /// Reading byte 0 is safe: the field is a slice of a larger buffer (same assumption as TracedInteger).
    const nautilus::val<int8_t> b0 = *(addr + nautilus::val<uint64_t>{0});
    const nautilus::val<uint64_t> isNeg = nautilus::val<uint64_t>(b0 == nautilus::val<int8_t>(static_cast<int8_t>('-')));
    const nautilus::val<uint64_t> isSign = isNeg + nautilus::val<uint64_t>(b0 == nautilus::val<int8_t>(static_cast<int8_t>('+')));

    nautilus::val<uint64_t> mant{0};
    nautilus::val<uint64_t> frac{0};
    nautilus::val<uint64_t> digits{0};
    nautilus::val<uint64_t> seenDot{0};
    nautilus::val<uint64_t> hard{0};
    hard = hard + nautilus::val<uint64_t>(size == nautilus::val<uint64_t>{0});

    for (nautilus::val<uint64_t> i = isSign; i < size; i = i + nautilus::val<uint64_t>{1})
    {
        const nautilus::val<int8_t> c = *(addr + i);
        const nautilus::val<uint64_t> d = nautilus::val<uint64_t>(c) - nautilus::val<uint64_t>{'0'};
        const nautilus::val<uint64_t> isDigit = nautilus::val<uint64_t>(d <= nautilus::val<uint64_t>{9});
        const nautilus::val<uint64_t> isDot = nautilus::val<uint64_t>(c == nautilus::val<int8_t>(static_cast<int8_t>('.')));
        /// branchless: mant = isDigit ? mant*10 + d : mant  (d wraps huge when c<'0', but masked by isDigit)
        mant = (mant * (nautilus::val<uint64_t>{1} + (nautilus::val<uint64_t>{9} * isDigit))) + (d * isDigit);
        digits = digits + isDigit;
        frac = frac + (isDigit * seenDot); /// count digits AFTER the '.'
        hard = hard + (isDot * seenDot); /// a second '.' is malformed
        seenDot = seenDot + isDot;
        hard = hard + ((nautilus::val<uint64_t>{1} - isDigit) * (nautilus::val<uint64_t>{1} - isDot)); /// 'e'/'E'/junk -> hard
    }
    hard = hard + nautilus::val<uint64_t>(digits == nautilus::val<uint64_t>{0});
    hard = hard + nautilus::val<uint64_t>(digits > nautilus::val<uint64_t>{Tr::MAX_FAST_DIGITS});
    hard = hard + nautilus::val<uint64_t>(frac > nautilus::val<uint64_t>{Tr::MAX_FRAC});

    nautilus::val<T> result{static_cast<T>(0)};
    if (hard != nautilus::val<uint64_t>{0})
    {
        result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFloatFallback<T>, addr, size);
    }
    else
    {
        const nautilus::val<int8_t*> tbl{reinterpret_cast<int8_t*>(const_cast<T*>(Tr::pow10()))};
        const nautilus::val<T> scale = readValueFromMemRef<T>(tbl + (frac * nautilus::val<uint64_t>{sizeof(T)}));
        nautilus::val<T> v = static_cast<nautilus::val<T>>(mant) / scale;
        /// negate if isNeg: v - 2*isNeg*v  (branchless; sign is highly predictable anyway)
        v = v - (static_cast<nautilus::val<T>>(nautilus::val<uint64_t>{2} * isNeg) * v);
        result = v;
    }
    return result;
}

template <typename T>
VarVal parseNullable(const nautilus::val<int8_t*>& addr, const nautilus::val<uint64_t>& size, const std::vector<std::string>& nullValues)
{
    const auto parseResult = NAUTILUS_TAGGED_INVOKE(
        "parse_null", parseFloatNullable<T>, addr, size, nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<T> value = *getMemberWithOffset<T>(parseResult, offsetof(TracedFloatParseResult<T>, value));
    const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(TracedFloatParseResult<T>, isNull));
    return VarVal{value, true, isNull};
}
} /// namespace

VarVal TracedFloatF64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        return parseNullable<double>(fieldAddress, fieldSize, nullValues);
    }
    return VarVal{tracedFastParse<double>(fieldAddress, fieldSize), false, false};
}

VarVal TracedFloatF64InputParser::parseLazyToVarVal(
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
            result = tracedFastParse<double>(fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    return VarVal{tracedFastParse<double>(fieldAddress, fieldSize), false, false};
}

VarVal TracedFloatF32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        return parseNullable<float>(fieldAddress, fieldSize, nullValues);
    }
    return VarVal{tracedFastParse<float>(fieldAddress, fieldSize), false, false};
}

VarVal TracedFloatF32InputParser::parseLazyToVarVal(
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
            result = tracedFastParse<float>(fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    return VarVal{tracedFastParse<float>(fieldAddress, fieldSize), false, false};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedFloatF32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedFloatF32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedFloatF64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedFloatF64InputParser>();
}
}
