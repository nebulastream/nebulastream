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

#include <FastIntegerInputParser.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <fast_float/fast_float.h>
#include <ErrorHandling.hpp>
#include <InputParserRegistry.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>

namespace NES
{

template <typename T>
struct FastIntParseResult
{
    T value;
    bool isNull;
};

/// Non-nullable: parse the whole field with fast_float. fast_float stops at the
/// first non-digit and still reports success, so the `ptr == last` full-field
/// consumption check is MANDATORY -- otherwise "12x" would parse as 12. Narrow
/// types are range-checked by fast_float (e.g. "300" into int8_t -> errc).
template <typename T, bool Nullable>
requires(not Nullable)
NAUTILUS_TAGGED_INLINE(input_parse)
T parseWithFastInt(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    T result;
    const char* fieldAddressAsChar = reinterpret_cast<const char*>(fieldAddress);
    if (auto [ptr, ec] = fast_float::from_chars(fieldAddressAsChar, fieldAddressAsChar + fieldSize, result);
        ec == std::errc() && ptr == fieldAddressAsChar + fieldSize)
    {
        return result;
    }
    throw CannotFormatMalformedStringValue("Error occured during fast integer parse.");
}

/// Nullable: a configured null marker -> isNull; an otherwise unparseable field
/// (or a partial parse) -> isNull, mirroring the FastFloat nullable path.
template <typename T, bool Nullable>
requires Nullable
FastIntParseResult<T>*
parseWithFastInt(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    thread_local FastIntParseResult<T> result;
    result.isNull = false;

    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }

    T parsedInt;
    const char* fieldAddressAsChar = reinterpret_cast<const char*>(fieldAddress);
    if (auto [ptr, ec] = fast_float::from_chars(fieldAddressAsChar, fieldAddressAsChar + fieldSize, parsedInt);
        ec == std::errc() && ptr == fieldAddressAsChar + fieldSize)
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

VarVal FastINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<int8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int8_t> nautilusValue = *getMemberWithOffset<int8_t>(parseResult, offsetof(FastIntParseResult<int8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<int8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int8_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<int8_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<int16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int16_t> nautilusValue = *getMemberWithOffset<int16_t>(parseResult, offsetof(FastIntParseResult<int16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<int16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int16_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<int16_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<int32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int32_t> nautilusValue = *getMemberWithOffset<int32_t>(parseResult, offsetof(FastIntParseResult<int32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<int32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int32_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<int32_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<int64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int64_t> nautilusValue = *getMemberWithOffset<int64_t>(parseResult, offsetof(FastIntParseResult<int64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<int64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<int64_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<int64_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastUINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<uint8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint8_t> nautilusValue = *getMemberWithOffset<uint8_t>(parseResult, offsetof(FastIntParseResult<uint8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<uint8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint8_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<uint8_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastUINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<uint16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint16_t> nautilusValue
            = *getMemberWithOffset<uint16_t>(parseResult, offsetof(FastIntParseResult<uint16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<uint16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint16_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<uint16_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastUINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<uint32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint32_t> nautilusValue
            = *getMemberWithOffset<uint32_t>(parseResult, offsetof(FastIntParseResult<uint32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<uint32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint32_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<uint32_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastUINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastInt<uint64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint64_t> nautilusValue
            = *getMemberWithOffset<uint64_t>(parseResult, offsetof(FastIntParseResult<uint64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastIntParseResult<uint64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<uint64_t> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastInt<uint64_t, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastINT64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastUINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastUINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastUINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastUINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastUINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastUINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastUINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastUINT64InputParser>();
}
}
