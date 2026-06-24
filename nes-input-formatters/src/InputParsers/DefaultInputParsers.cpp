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
#include <InputParsers/DefaultInputParsers.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string_view>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <Util/Strings.hpp>
#include <InputParserRegistry.hpp>
#include <RawValueParser.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// TMP DIAGNOSTIC: estimate the value-parse body cost (string + trim + from_chars) per field, plus the
/// exact call count, printed at process exit. Timing *every* call distorts the run badly: at hundreds of
/// millions of parses the two steady_clock::now() reads per call dominate wall time (an all-fields proj
/// ran ~3x slower than an uninstrumented build, and the distortion scales with the parse count). So we
/// SAMPLE -- only one in g_parseSamplePeriod parses is timed (default 1024) -- and scale the sampled time
/// by calls/sampled to estimate parse_cpu. The parse body is uniform per field, so the sample is unbiased.
/// Each worker thread keeps its own counters (no sharing on the hot path) and flushes them into the globals
/// at thread exit (worker threads join before this printer's static dtor runs). Set NES_PARSE_SAMPLE_PERIOD=1
/// to time every call (the old, run-distorting behaviour) for an A/B against the sampled estimate. REVERT
/// before merge.
std::atomic<std::uint64_t> g_parseNs{0}; /// summed body time (ns) over the timed (sampled) calls only
std::atomic<std::uint64_t> g_parseCalls{0}; /// total parse calls (exact)
std::atomic<std::uint64_t> g_parseSampled{0}; /// number of timed (sampled) calls

const std::uint64_t g_parseSamplePeriod = []() -> std::uint64_t
{
    if (const char* env = std::getenv("NES_PARSE_SAMPLE_PERIOD"))
    {
        const auto period = static_cast<std::uint64_t>(std::strtoull(env, nullptr, 10));
        return period == 0 ? std::uint64_t{1} : period;
    }
    return std::uint64_t{1024};
}();

struct ParseAccum
{
    std::uint64_t ns{0};
    std::uint64_t calls{0};
    std::uint64_t sampled{0};
    std::uint64_t sinceLast{0};

    ~ParseAccum()
    {
        g_parseNs.fetch_add(ns, std::memory_order_relaxed);
        g_parseCalls.fetch_add(calls, std::memory_order_relaxed);
        g_parseSampled.fetch_add(sampled, std::memory_order_relaxed);
    }
};

thread_local ParseAccum t_parseAccum;

const struct ParseDiagPrinter
{
    ~ParseDiagPrinter()
    {
        const auto sampledNs = g_parseNs.load();
        const auto calls = g_parseCalls.load();
        const auto sampled = g_parseSampled.load();
        /// Scale the sampled body time up to all calls (uniform per-field body -> unbiased estimate).
        const double estParseSeconds
            = (sampled == 0) ? 0.0 : (static_cast<double>(sampledNs) / 1e9) * (static_cast<double>(calls) / static_cast<double>(sampled));
        std::fprintf(
            stderr,
            "DIAG_PARSE parse_cpu~%.4fs calls=%llu sampled=%llu (1/%llu)\n",
            estParseSeconds,
            static_cast<unsigned long long>(calls),
            static_cast<unsigned long long>(sampled),
            static_cast<unsigned long long>(g_parseSamplePeriod));
    }
} g_parseDiagPrinter;
}

template <typename T>
struct ParseResult
{
    T value;
    bool isNull;
};

/// Non-nullable: returns T directly, no thread_local, no null handling.
template <typename T, bool Nullable>
requires(not Nullable)
NAUTILUS_TAGGED_INLINE(input_parse)
T parseFixedSized(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    /// TMP DIAGNOSTIC: NES_SKIP_PARSE returns a dummy value, skipping the std::string construction +
    /// trim + from_chars. The throughput delta vs a normal run isolates the value-parse phase cost
    /// (per field per tuple). Breaks results -- diagnostic only. REVERT before merge.
    static const bool skipParse = std::getenv("NES_SKIP_PARSE") != nullptr;
    if (skipParse)
    {
        return T{0};
    }
    /// TMP DIAGNOSTIC: NES_PARSE_LEGACY_ALLOC selects the old path (copy each field into a heap std::string
    /// before trim+parse) so the allocation-free path can be A/B'd against it in one thermal window.
    /// REVERT before merge.
    static const bool legacyAlloc = std::getenv("NES_PARSE_LEGACY_ALLOC") != nullptr;
    /// Sample only one in g_parseSamplePeriod calls (see g_parseDiagPrinter); a clock read on every call
    /// would otherwise dominate wall time at high parse counts.
    t_parseAccum.calls += 1;
    const bool timeThis = (++t_parseAccum.sinceLast >= g_parseSamplePeriod);
    std::chrono::steady_clock::time_point t0;
    if (timeThis)
    {
        t_parseAccum.sinceLast = 0;
        t0 = std::chrono::steady_clock::now();
    }
    T result{};
    if (legacyAlloc)
    {
        const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
        result = NES::from_chars_with_exception<T>(trimWhiteSpaces(fieldAsString));
    }
    else
    {
        /// Allocation-free: parse straight off the raw buffer bytes. trimWhiteSpaces returns a string_view
        /// (no copy) and from_chars_with_exception takes a string_view, so the std::string copy of every
        /// field was pure overhead. fieldAddress points into the source's raw byte buffer.
        const std::string_view fieldView{reinterpret_cast<const char*>(fieldAddress), fieldSize};
        result = NES::from_chars_with_exception<T>(trimWhiteSpaces(fieldView));
    }
    if (timeThis)
    {
        t_parseAccum.ns += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count());
        t_parseAccum.sampled += 1;
    }
    return result;
}

/// Nullable: returns ParseResult<T>* via thread_local, with null checking.
template <typename T, bool Nullable>
requires Nullable
ParseResult<T>* parseFixedSized(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    thread_local ParseResult<T> result;
    result.isNull = false;

    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }

    try
    {
        result.value = parseFixedSized<T, false>(fieldAddress, fieldSize);
    }
    catch (const Exception& ex)
    {
        result.isNull = true;
        result.value = T{0};
    }
    return &result;
}

VarVal DefaultBOOLInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<bool, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<bool> nautilusValue = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<bool>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<bool>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<bool> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<bool, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultBOOLInputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<bool> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<bool, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<bool> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<bool, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultCHARInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<char, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<char> nautilusValue = *getMemberWithOffset<char>(parseResult, offsetof(ParseResult<char>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<char>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<char> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<char, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultCHARInputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    if (nullable)
    {
        nautilus::val<char> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<char, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<char> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<char, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultF32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<float, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<float> nautilusValue = *getMemberWithOffset<float>(parseResult, offsetof(ParseResult<float>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<float>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<float> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<float, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultF32InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<float, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<float> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<float, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultF64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<double, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<double> nautilusValue = *getMemberWithOffset<double>(parseResult, offsetof(ParseResult<double>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<double>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<double> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<double, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultF64InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<double, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<double> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<double, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<int8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int8_t> nautilusValue = *getMemberWithOffset<int8_t>(parseResult, offsetof(ParseResult<int8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int8_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int8_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT8InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int8_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int8_t> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int8_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<int16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int16_t> nautilusValue = *getMemberWithOffset<int16_t>(parseResult, offsetof(ParseResult<int16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int16_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int16_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT16InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int16_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int16_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int16_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<int32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int32_t> nautilusValue = *getMemberWithOffset<int32_t>(parseResult, offsetof(ParseResult<int32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int32_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int32_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT32InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int32_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int32_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int32_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<int64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int64_t> nautilusValue = *getMemberWithOffset<int64_t>(parseResult, offsetof(ParseResult<int64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int64_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int64_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT64InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int64_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<int64_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<int64_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultUINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<uint8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint8_t> nautilusValue = *getMemberWithOffset<uint8_t>(parseResult, offsetof(ParseResult<uint8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint8_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint8_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT8InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint8_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint8_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint8_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultUINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<uint16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint16_t> nautilusValue = *getMemberWithOffset<uint16_t>(parseResult, offsetof(ParseResult<uint16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint16_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint16_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT16InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint16_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint16_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint16_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultUINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<uint32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint32_t> nautilusValue = *getMemberWithOffset<uint32_t>(parseResult, offsetof(ParseResult<uint32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint32_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint32_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT32InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint32_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint32_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint32_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal DefaultUINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseFixedSized<uint64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint64_t> nautilusValue = *getMemberWithOffset<uint64_t>(parseResult, offsetof(ParseResult<uint64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint64_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint64_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT64InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint64_t, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<uint64_t> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseFixedSized<uint64_t, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultBOOLInputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultBOOLInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultCHARInputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultCHARInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultF32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultF32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultF64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultF64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultINT64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultUINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultUINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultUINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultUINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT64InputParser>();
}
}
