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
#include <string_view>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Util/Strings.hpp>
#include <InputParserRegistry.hpp>
#include <InputParserUtil.hpp>
#include <function.hpp>
#include <select.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultInputParser
{
template <typename T>
struct ParseResult
{
    T value;
    bool isNull;
};

/// We expect a pointer and the size so that we can use this method from the nautilus runtime
bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    return std::ranges::any_of(*nullValues, [fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

/// Returns the native NULL representation for a primitive datatype (represented as nautilus::val...)
template <typename T>
VarVal writePrimitiveNull()
{
    return VarVal{nautilus::val<T>{0}, true, true};
}

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

struct TruncateSpacesResult
{
    const int8_t* ptr;
    uint64_t size;
};

/// Obtains the pointer and size of a field.
/// Returns a pointer and the size of the field without the trailing whitespaces
TruncateSpacesResult* truncateTrailingSpaces(const int8_t* ptr, const uint64_t size)
{
    thread_local static TruncateSpacesResult result;
    std::string_view uncutString{reinterpret_cast<const char*>(ptr), size};
    if (const auto lastNonWs = uncutString.find_last_not_of(" \t\n\r"); lastNonWs != std::string_view::npos)
    {
        uncutString = uncutString.substr(0, lastNonWs + 1);
    }
    result.ptr = reinterpret_cast<const int8_t*>(uncutString.data());
    result.size = uncutString.size();
    return &result;
}

}

namespace NES
{

VarVal DefaultBOOLInputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<bool, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<bool> nautilusValue
        = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<bool>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultBOOLInputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<bool> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<bool>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<bool, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<bool>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<bool>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultBOOLInputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<bool>();
}

VarVal DefaultCHARInputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    /// Cut off quotes if neccessary
    const nautilus::val<int8_t*> trueFieldAddress = quoted ? fieldAddress + nautilus::val<uint32_t>(1) : fieldAddress;
    const nautilus::val<uint64_t> trueFieldSize = quoted ? fieldSize - nautilus::val<uint32_t>(2) : fieldSize;

    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<char, false>,
        trueFieldAddress,
        trueFieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<char> nautilusValue
        = *getMemberWithOffset<char>(parseResult, offsetof(DefaultInputParser::ParseResult<char>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultCHARInputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<char> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<char>>();
        isNull = true;
    }
    else
    {
        /// Cut off quotes if neccessary
        const nautilus::val<int8_t*> trueFieldAddress = quoted ? fieldAddress + nautilus::val<uint32_t>(1) : fieldAddress;
        const nautilus::val<uint64_t> trueFieldSize = quoted ? fieldSize - nautilus::val<uint32_t>(2) : fieldSize;

        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<char, true>,
            trueFieldAddress,
            trueFieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<char>(parseResult, offsetof(DefaultInputParser::ParseResult<char>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<char>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultCHARInputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<char>();
}

VarVal DefaultF32InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<float, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<float> nautilusValue
        = *getMemberWithOffset<float>(parseResult, offsetof(DefaultInputParser::ParseResult<float>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultF32InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<float> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<float>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<float, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<float>(parseResult, offsetof(DefaultInputParser::ParseResult<float>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<float>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultF32InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<float>();
}

VarVal DefaultF64InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<double, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<double> nautilusValue
        = *getMemberWithOffset<double>(parseResult, offsetof(DefaultInputParser::ParseResult<double>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultF64InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<double> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<double>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<double, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<double>(parseResult, offsetof(DefaultInputParser::ParseResult<double>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<double>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultF64InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<double>();
}

VarVal DefaultINT8InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int8_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int8_t> nautilusValue
        = *getMemberWithOffset<int8_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int8_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT8InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<int8_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<int8_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<int8_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int8_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<int8_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultINT8InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<int8_t>();
}

VarVal DefaultINT16InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int16_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int16_t> nautilusValue
        = *getMemberWithOffset<int16_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int16_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT16InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<int16_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<int16_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<int16_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int16_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<int16_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultINT16InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<int16_t>();
}

VarVal DefaultINT32InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int32_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int32_t> nautilusValue
        = *getMemberWithOffset<int32_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int32_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT32InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<int32_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<int32_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<int32_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int32_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<int32_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultINT32InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<int32_t>();
}

VarVal DefaultINT64InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<int64_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int64_t> nautilusValue
        = *getMemberWithOffset<int64_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int64_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT64InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<int64_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<int64_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<int64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<int64_t>(parseResult, offsetof(DefaultInputParser::ParseResult<int64_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<int64_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultINT64InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<int64_t>();
}

VarVal DefaultUINT8InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint8_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint8_t> nautilusValue
        = *getMemberWithOffset<uint8_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint8_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT8InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<uint8_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<uint8_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint8_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<uint8_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint8_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<uint8_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultUINT8InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<uint8_t>();
}

VarVal DefaultUINT16InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint16_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint16_t> nautilusValue
        = *getMemberWithOffset<uint16_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint16_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT16InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<uint16_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<uint16_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint16_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<uint16_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint16_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<uint16_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultUINT16InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<uint16_t>();
}

VarVal DefaultUINT32InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint32_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint32_t> nautilusValue
        = *getMemberWithOffset<uint32_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint32_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT32InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<uint32_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<uint32_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint32_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<uint32_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint32_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<uint32_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultUINT32InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<uint32_t>();
}

VarVal DefaultUINT64InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>&) const
{
    const auto parseResult = nautilus::invoke(
        DefaultInputParser::parseFixedSized<uint64_t, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint64_t> nautilusValue
        = *getMemberWithOffset<uint64_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint64_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT64InputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<uint64_t> nautilusValue;
    nautilus::val<bool> isNull;
    if (fieldNotFound)
    {
        nautilusValue = writeNull().getRawValueAs<nautilus::val<uint64_t>>();
        isNull = true;
    }
    else
    {
        const auto parseResult = nautilus::invoke(
            DefaultInputParser::parseFixedSized<uint64_t, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<uint64_t>(parseResult, offsetof(DefaultInputParser::ParseResult<uint64_t>, value));
        isNull = *getMemberWithOffset<bool>(parseResult, offsetof(DefaultInputParser::ParseResult<uint64_t>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal NullableDefaultUINT64InputParser::writeNull() const
{
    return DefaultInputParser::writePrimitiveNull<uint64_t>();
}

VarVal DefaultVARSIZEDInputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const nautilus::val<bool>&) const
{
    nautilus::val<int8_t*> trueFieldAddress = fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    if (hasTrailingSpace)
    {
        /// Cut off trailing whitespace
        const auto truncatedField = nautilus::invoke(DefaultInputParser::truncateTrailingSpaces, fieldAddress, fieldSize);
        trueFieldAddress = *getMemberWithOffset<int8_t*>(truncatedField, offsetof(DefaultInputParser::TruncateSpacesResult, ptr));
        trueFieldSize = *getMemberWithOffset<uint64_t>(truncatedField, offsetof(DefaultInputParser::TruncateSpacesResult, size));
    }
    if (quoted)
    {
        /// Cut off quotes
        trueFieldAddress = trueFieldAddress + nautilus::val<uint32_t>(1);
        trueFieldSize = trueFieldSize - nautilus::val<uint64_t>(2);
    }
    const VariableSizedData varsized{trueFieldAddress, trueFieldSize};
    return VarVal{varsized, false, false};
}

VarVal NullableDefaultVARSIZEDInputParser::parseToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const nautilus::val<bool>& fieldNotFound) const
{
    nautilus::val<int8_t*> trueFieldAddress = fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    nautilus::val<bool> isNull = false;
    if (fieldNotFound)
    {
        const auto nullVarsized = writeNull().getRawValueAs<VariableSizedData>();
        trueFieldAddress = nullVarsized.getContent();
        trueFieldSize = nullVarsized.getSize();
        isNull = true;
    }
    else
    {
        if (hasTrailingSpace)
        {
            /// Cut off trailing whitespace
            const auto truncatedField = nautilus::invoke(DefaultInputParser::truncateTrailingSpaces, fieldAddress, fieldSize);
            trueFieldAddress = *getMemberWithOffset<int8_t*>(truncatedField, offsetof(DefaultInputParser::TruncateSpacesResult, ptr));
            trueFieldSize = *getMemberWithOffset<uint64_t>(truncatedField, offsetof(DefaultInputParser::TruncateSpacesResult, size));
        }
        if (nautilus::invoke(
                {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
                DefaultInputParser::checkIsNullProxy,
                trueFieldAddress,
                trueFieldSize,
                nautilus::val<const std::vector<std::string>*>{&nullValues}))
        {
            /// Field contains a null value
            const auto nullVarsized = writeNull().getRawValueAs<VariableSizedData>();
            trueFieldAddress = nullVarsized.getContent();
            trueFieldSize = nullVarsized.getSize();
            isNull = true;
        }
        else
        {
            if (quoted)
            {
                /// Cut off quotes
                trueFieldAddress = trueFieldAddress + nautilus::val<uint32_t>(1);
                trueFieldSize = trueFieldSize - nautilus::val<uint64_t>(2);
            }
        }
    }
    const VariableSizedData varsized{trueFieldAddress, trueFieldSize};
    return VarVal{varsized, true, isNull};
}

VarVal NullableDefaultVARSIZEDInputParser::writeNull() const
{
    return VarVal{VariableSizedData{nautilus::val<int8_t*>{nullptr}, 0}, true, true};
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultBOOLInputParser(InputParserRegistryArguments)
{
    return std::make_unique<DefaultBOOLInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultCHARInputParser(InputParserRegistryArguments args)
{
    return std::make_unique<DefaultCHARInputParser>(args.quotedText);
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

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterDefaultVARSIZEDInputParser(InputParserRegistryArguments args)
{
    return std::make_unique<DefaultVARSIZEDInputParser>(args.quotedText, args.hasTrailingSpaces);
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultBOOLInputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultBOOLInputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultCHARInputParser(InputParserRegistryArguments args)
{
    return std::make_unique<NullableDefaultCHARInputParser>(args.quotedText);
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultF32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultF32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultF64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultF64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultINT64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultUINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultUINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultUINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultUINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterNullableDefaultVARSIZEDInputParser(InputParserRegistryArguments args)
{
    return std::make_unique<NullableDefaultVARSIZEDInputParser>(args.quotedText, args.hasTrailingSpaces);
}
}
