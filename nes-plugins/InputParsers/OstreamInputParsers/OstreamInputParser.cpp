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

#include <OstreamInputParser.hpp>

#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/InvokeMacro.hpp>
#include <ErrorHandling.hpp>
#include <InputParserRegistry.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>

namespace NES
{

template <typename T>
struct OstreamParseResult
{
    T value;
    bool isNull;
};

/// Opaque (non-inlined) invoke: iostream locale statics must not be spliced into the JIT module.
template <typename T, bool Nullable>
requires(not Nullable)
T parseWithOstream(const int8_t* fieldAddress, const uint64_t fieldSize)
{
    const std::string field(reinterpret_cast<const char*>(fieldAddress), fieldSize);
    std::istringstream iss(field);
    T result{};
    if ((iss >> result))
    {
        return result;
    }
    throw CannotFormatMalformedStringValue("Error occured during ostream float parse.");
}

template <typename T, bool Nullable>
requires Nullable
OstreamParseResult<T>* parseWithOstream(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    thread_local OstreamParseResult<T> result;
    result.isNull = false;

    if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
    {
        result.isNull = true;
        result.value = T{0};
        return &result;
    }

    const std::string field(reinterpret_cast<const char*>(fieldAddress), fieldSize);
    std::istringstream iss(field);
    T parsed{};
    if ((iss >> parsed))
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

template <typename T>
static VarVal parseToVarValImpl(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues)
{
    if (nullable)
    {
        const auto parseResult = NAUTILUS_TAGGED_INVOKE(
            "parse_null", parseWithOstream<T, true>, fieldAddress, fieldSize, nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(OstreamParseResult<T>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(OstreamParseResult<T>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const nautilus::val<T> nautilusValue = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithOstream<T, false>, fieldAddress, fieldSize);
    return VarVal{nautilusValue, nullable, false};
}

template <typename T>
static VarVal parseLazyToVarValImpl(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize)
{
    if (nullable)
    {
        nautilus::val<T> result{0};
        if (!isNull)
        {
            result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithOstream<T, false>, fieldAddress, fieldSize);
        }
        return VarVal{result, true, isNull};
    }
    const nautilus::val<T> result = NAUTILUS_TAGGED_INVOKE("parse_not_null", parseWithOstream<T, false>, fieldAddress, fieldSize);
    return VarVal{result, false, false};
}

VarVal OstreamF32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    return parseToVarValImpl<float>(nullable, fieldAddress, fieldSize, nullValues);
}

VarVal OstreamF32InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    return parseLazyToVarValImpl<float>(nullable, isNull, fieldAddress, fieldSize);
}

VarVal OstreamF64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    return parseToVarValImpl<double>(nullable, fieldAddress, fieldSize, nullValues);
}

VarVal OstreamF64InputParser::parseLazyToVarVal(
    const bool& nullable,
    const nautilus::val<bool>& isNull,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize) const
{
    return parseLazyToVarValImpl<double>(nullable, isNull, fieldAddress, fieldSize);
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterOstreamF32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<OstreamF32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterOstreamF64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<OstreamF64InputParser>();
}
}
