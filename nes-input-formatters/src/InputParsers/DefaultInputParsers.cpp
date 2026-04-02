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

#include <memory>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Util/InvokeMacro.hpp>
#include <Util/Strings.hpp>
#include <InputParserRegistry.hpp>
#include <RawValueParser.hpp>
#include <function.hpp>
#include <val_ptr.hpp>
#include <Util/InlineTagMacro.hpp>

namespace NES
{
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
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    const auto trimmedFieldAsString = trimWhiteSpaces(fieldAsString);
    return NES::from_chars_with_exception<T>(trimmedFieldAsString);
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
