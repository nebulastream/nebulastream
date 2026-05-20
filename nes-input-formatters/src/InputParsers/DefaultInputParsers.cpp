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
#include <Util/Strings.hpp>
#include <InputParserRegistry.hpp>
#include <InputParserUtil.hpp>
#include <function.hpp>
#include <select.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultInputParser
{

template <typename T, bool Nullable>
ParseResult<T>* parseFixedSized(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static ParseResult<T> result;
    result.isNull = false;

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
        {
            result.isNull = true;
            result.value = T{0};
            return &result;
        }
    }

    try
    {
        const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
        const auto trimmedFieldAsString = trimWhiteSpaces(fieldAsString);
        result.value = NES::from_chars_with_exception<T>(trimmedFieldAsString);
    }
    catch (const Exception& ex)
    {
        /// If the field is nullable, we return a null value, otherwise we throw an exception
        if constexpr (not Nullable)
        {
            throw;
        }
        result.isNull = true;
        result.value = T{0};
    }
    return &result;
}
}

namespace NES
{

VarVal DefaultBOOLInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<bool, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<bool> nautilusValue = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<bool>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<bool>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<bool, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<bool> nautilusValue = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<bool>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultBOOLInputParser::writeNull() const
{
    return writePrimitiveNull<bool>();
}

VarVal DefaultCHARInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<char, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<char> nautilusValue = *getMemberWithOffset<char>(parseResult, offsetof(ParseResult<char>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<char>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<char, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<char> nautilusValue = *getMemberWithOffset<char>(parseResult, offsetof(ParseResult<char>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultCHARInputParser::writeNull() const
{
    return writePrimitiveNull<char>();
}

VarVal QuotedCHARInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<char, true>,
            fieldAddress + nautilus::val<uint32_t>(1),
            fieldSize - nautilus::val<uint32_t>(2),
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<char> nautilusValue = *getMemberWithOffset<char>(parseResult, offsetof(ParseResult<char>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<char>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<char, false>,
        fieldAddress + nautilus::val<uint32_t>(1),
        fieldSize - nautilus::val<uint32_t>(2),
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<char> nautilusValue = *getMemberWithOffset<char>(parseResult, offsetof(ParseResult<char>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal QuotedCHARInputParser::writeNull() const
{
    return writePrimitiveNull<char>();
}

VarVal DefaultF32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<float, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<float> nautilusValue = *getMemberWithOffset<float>(parseResult, offsetof(ParseResult<float>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<float>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<float, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<float> nautilusValue = *getMemberWithOffset<float>(parseResult, offsetof(ParseResult<float>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultF32InputParser::writeNull() const
{
    return writePrimitiveNull<float>();
}

VarVal DefaultF64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<double, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<double> nautilusValue = *getMemberWithOffset<double>(parseResult, offsetof(ParseResult<double>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<double>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<double, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<double> nautilusValue = *getMemberWithOffset<double>(parseResult, offsetof(ParseResult<double>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultF64InputParser::writeNull() const
{
    return writePrimitiveNull<double>();
}

VarVal DefaultINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int8_t> nautilusValue = *getMemberWithOffset<int8_t>(parseResult, offsetof(ParseResult<int8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int8_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int8_t> nautilusValue = *getMemberWithOffset<int8_t>(parseResult, offsetof(ParseResult<int8_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT8InputParser::writeNull() const
{
    return writePrimitiveNull<int8_t>();
}

VarVal DefaultINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int16_t> nautilusValue = *getMemberWithOffset<int16_t>(parseResult, offsetof(ParseResult<int16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int16_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int16_t> nautilusValue = *getMemberWithOffset<int16_t>(parseResult, offsetof(ParseResult<int16_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT16InputParser::writeNull() const
{
    return writePrimitiveNull<int16_t>();
}

VarVal DefaultINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int32_t> nautilusValue = *getMemberWithOffset<int32_t>(parseResult, offsetof(ParseResult<int32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int32_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int32_t> nautilusValue = *getMemberWithOffset<int32_t>(parseResult, offsetof(ParseResult<int32_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT32InputParser::writeNull() const
{
    return writePrimitiveNull<int32_t>();
}

VarVal DefaultINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<int64_t> nautilusValue = *getMemberWithOffset<int64_t>(parseResult, offsetof(ParseResult<int64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<int64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int64_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int64_t> nautilusValue = *getMemberWithOffset<int64_t>(parseResult, offsetof(ParseResult<int64_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultINT64InputParser::writeNull() const
{
    return writePrimitiveNull<int64_t>();
}

VarVal DefaultUINT8InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint8_t> nautilusValue = *getMemberWithOffset<uint8_t>(parseResult, offsetof(ParseResult<uint8_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint8_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint8_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint8_t> nautilusValue = *getMemberWithOffset<uint8_t>(parseResult, offsetof(ParseResult<uint8_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT8InputParser::writeNull() const
{
    return writePrimitiveNull<uint8_t>();
}

VarVal DefaultUINT16InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint16_t> nautilusValue = *getMemberWithOffset<uint16_t>(parseResult, offsetof(ParseResult<uint16_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint16_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint16_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint16_t> nautilusValue = *getMemberWithOffset<uint16_t>(parseResult, offsetof(ParseResult<uint16_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT16InputParser::writeNull() const
{
    return writePrimitiveNull<uint16_t>();
}

VarVal DefaultUINT32InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint32_t> nautilusValue = *getMemberWithOffset<uint32_t>(parseResult, offsetof(ParseResult<uint32_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint32_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint32_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint32_t> nautilusValue = *getMemberWithOffset<uint32_t>(parseResult, offsetof(ParseResult<uint32_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT32InputParser::writeNull() const
{
    return writePrimitiveNull<uint32_t>();
}

VarVal DefaultUINT64InputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        const nautilus::val<uint64_t> nautilusValue = *getMemberWithOffset<uint64_t>(parseResult, offsetof(ParseResult<uint64_t>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<uint64_t>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint64_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint64_t> nautilusValue = *getMemberWithOffset<uint64_t>(parseResult, offsetof(ParseResult<uint64_t>, value));
    return VarVal{nautilusValue, nullable, false};
}

VarVal DefaultUINT64InputParser::writeNull() const
{
    return writePrimitiveNull<uint64_t>();
}

VarVal DefaultVARSIZEDInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    nautilus::val<bool> isNull{false};
    if (nullable)
    {
        isNull = nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
            checkIsNullProxy,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
    }
    const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress);
    const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize);
    const VariableSizedData varSized{ptr, size};
    return VarVal{varSized, nullable, isNull};
}

VarVal DefaultVARSIZEDInputParser::writeNull() const
{
    return VarVal{VariableSizedData{nautilus::val<int8_t*>{nullptr}, 0}, true, true};
}

VarVal QuotedVARSIZEDInputParser::parseToVarVal(
    bool nullable,
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues) const
{
    nautilus::val<bool> isNull{false};
    if (nullable)
    {
        isNull = nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
            checkIsNullProxy,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
    }
    const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress + nautilus::val<uint32_t>(1));
    const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize - nautilus::val<uint64_t>(2));
    const VariableSizedData varSized{ptr, size};
    return VarVal{varSized, nullable, isNull};
}

VarVal QuotedVARSIZEDInputParser::writeNull() const
{
    return VarVal{VariableSizedData{nautilus::val<int8_t*>{nullptr}, 0}, true, true};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultBOOLInputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultBOOLInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultCHARInputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultCHARInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterQuotedCHARInputParser(InputParserRegistryArguments)
{
    return std::make_unique<QuotedCHARInputParser>();
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

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultVARSIZEDInputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultVARSIZEDInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterQuotedVARSIZEDInputParser(InputParserRegistryArguments)
{
    return std::make_unique<QuotedVARSIZEDInputParser>();
}
}
