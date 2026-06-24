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

#include <FastFloatInputParser.hpp>

#include <memory>
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
struct FastFloatParseResult
{
    T value;
    bool isNull;
};

template <typename T, bool Nullable>
requires(not Nullable)
NAUTILUS_TAGGED_INLINE(input_parse)
T parseWithFastFloat(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    T result;
    const char* fieldAddressAsChar = reinterpret_cast<const char*>(fieldAddress);
    if (auto [ptr, ec] = fast_float::from_chars(fieldAddressAsChar, fieldAddressAsChar + fieldSize, result); ec == std::errc())
    {
        return result;
    }
    throw CannotFormatMalformedStringValue("Error occured during fast float parse.");
}

template <typename T, bool Nullable>
requires Nullable
FastFloatParseResult<T>*
parseWithFastFloat(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    thread_local FastFloatParseResult<T> result;
    result.isNull = false;

    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }

    T parsedFloat;
    const char* fieldAddressAsChar = reinterpret_cast<const char*>(fieldAddress);
    if (auto [ptr, ec] = fast_float::from_chars(fieldAddressAsChar, fieldAddressAsChar + fieldSize, parsedFloat); ec == std::errc())
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

VarVal FastFloatF64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastFloat<double, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<double> nautilusValue
            = *getMemberWithOffset<double>(parseResult, offsetof(FastFloatParseResult<double>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastFloatParseResult<double>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<double> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastFloat<double, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastFloatF64InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastFloat<double, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<double> result
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastFloat<double, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal FastFloatF32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null",
            parseWithFastFloat<float, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<float> nautilusValue = *getMemberWithOffset<float>(parseResult, offsetof(FastFloatParseResult<float>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(FastFloatParseResult<float>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<float> nautilusValue
        = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastFloat<float, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

VarVal FastFloatF32InputParser::parseLazyToVarVal(
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
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastFloat<float, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<float> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithFastFloat<float, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastFloatF32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastFloatF32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterFastFloatF64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<FastFloatF64InputParser>();
}
}
