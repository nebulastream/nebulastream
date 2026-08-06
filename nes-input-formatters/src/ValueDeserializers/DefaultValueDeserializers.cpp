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
#include <ValueDeserializers/DefaultValueDeserializers.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES::DefaultValueDeserializer
{
template <typename T>
struct DeserializedResult
{
    T value;
    bool isNull;
};

/// We expect a pointer and the size so that we can use this method from the nautilus runtime
bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    if (fieldAddress == nullptr && fieldSize == 0)
    {
        return true;
    }

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

template <typename T, bool Nullable, bool Quoted>
DeserializedResult<T>*
deserializeFixedSized(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static DeserializedResult<T> result;
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

    /// Remove the quotes from the address, if quoted.
    const int8_t* trueFieldAddress = Quoted ? fieldAddress + 1 : fieldAddress;
    const uint64_t trueFieldSize = Quoted ? fieldSize - 2 : fieldSize;

    try
    {
        const std::string fieldAsString{trueFieldAddress, trueFieldAddress + trueFieldSize};
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

VarVal DefaultBOOLValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<bool, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<bool> nautilusValue
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<bool>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultBOOLValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<bool, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<bool> nautilusValue
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<bool>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<bool>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    nautilus::val<char> nautilusValue{char{0}};
    if (quoted)
    {
        const auto deserializedResult = nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, false, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<char>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<char>, value));
    }
    else
    {
        const auto deserializedResult = nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, false, false>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<char>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<char>, value));
    }
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    nautilus::val<char> nautilusValue{char{0}};
    nautilus::val<bool> isNull{false};
    if (quoted)
    {
        const auto deserializedResult = nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, true, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<char>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<char>, value));
        isNull = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<char>, isNull));
    }
    else
    {
        const auto deserializedResult = nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, true, false>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues});
        nautilusValue = *getMemberWithOffset<char>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<char>, value));
        isNull = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<char>, isNull));
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultF32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<float, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<float> nautilusValue
        = *getMemberWithOffset<float>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<float>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultF32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<float, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<float> nautilusValue
        = *getMemberWithOffset<float>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<float>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<float>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultF64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<double, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<double> nautilusValue
        = *getMemberWithOffset<double>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<double>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultF64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<double, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<double> nautilusValue
        = *getMemberWithOffset<double>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<double>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<double>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int8_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int8_t> nautilusValue
        = *getMemberWithOffset<int8_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int8_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int8_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int8_t> nautilusValue
        = *getMemberWithOffset<int8_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int8_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int8_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int16_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int16_t> nautilusValue
        = *getMemberWithOffset<int16_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int16_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int16_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int16_t> nautilusValue
        = *getMemberWithOffset<int16_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int16_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int16_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int32_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int32_t> nautilusValue
        = *getMemberWithOffset<int32_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int32_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int32_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int32_t> nautilusValue
        = *getMemberWithOffset<int32_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int32_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int32_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int64_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int64_t> nautilusValue
        = *getMemberWithOffset<int64_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int64_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int64_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<int64_t> nautilusValue
        = *getMemberWithOffset<int64_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int64_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<int64_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint8_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint8_t> nautilusValue
        = *getMemberWithOffset<uint8_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint8_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint8_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint8_t> nautilusValue
        = *getMemberWithOffset<uint8_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint8_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint8_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint16_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint16_t> nautilusValue
        = *getMemberWithOffset<uint16_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint16_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint16_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint16_t> nautilusValue
        = *getMemberWithOffset<uint16_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint16_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint16_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint32_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint32_t> nautilusValue
        = *getMemberWithOffset<uint32_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint32_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint32_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint32_t> nautilusValue
        = *getMemberWithOffset<uint32_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint32_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint32_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint64_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint64_t> nautilusValue
        = *getMemberWithOffset<uint64_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint64_t>, value));
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    const auto deserializedResult = nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint64_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues});
    const nautilus::val<uint64_t> nautilusValue
        = *getMemberWithOffset<uint64_t>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint64_t>, value));
    const nautilus::val<bool> isNull
        = *getMemberWithOffset<bool>(deserializedResult, offsetof(DefaultValueDeserializer::DeserializedResult<uint64_t>, isNull));
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    nautilus::val<int8_t*> trueFieldAddress = fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    if (hasTrailingSpaces)
    {
        /// Cut off trailing whitespace
        const auto truncatedField = nautilus::invoke(DefaultValueDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize);
        trueFieldAddress = *getMemberWithOffset<int8_t*>(truncatedField, offsetof(DefaultValueDeserializer::TruncateSpacesResult, ptr));
        trueFieldSize = *getMemberWithOffset<uint64_t>(truncatedField, offsetof(DefaultValueDeserializer::TruncateSpacesResult, size));
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

VarVal NullableDefaultVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>& nullValues) const
{
    nautilus::val<int8_t*> trueFieldAddress = fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    nautilus::val<bool> isNull = false;
    if (hasTrailingSpaces)
    {
        /// Cut off trailing whitespace
        const auto truncatedField = nautilus::invoke(DefaultValueDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize);
        trueFieldAddress = *getMemberWithOffset<int8_t*>(truncatedField, offsetof(DefaultValueDeserializer::TruncateSpacesResult, ptr));
        trueFieldSize = *getMemberWithOffset<uint64_t>(truncatedField, offsetof(DefaultValueDeserializer::TruncateSpacesResult, size));
    }
    if (nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
            DefaultValueDeserializer::checkIsNullProxy,
            trueFieldAddress,
            trueFieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues}))
    {
        /// Field contains a null value
        trueFieldAddress = nautilus::val<int8_t*>{nullptr};
        trueFieldSize = nautilus::val<uint64_t>{0};
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
    const VariableSizedData varsized{trueFieldAddress, trueFieldSize};
    return VarVal{varsized, true, isNull};
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultBOOLValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultBOOLValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultCHARValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultCHARValueDeserializer>(args.quoted);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultF32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultF32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultF64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultF64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultVARSIZEDValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultVARSIZEDValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultBOOLValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultBOOLValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultCHARValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<NullableDefaultCHARValueDeserializer>(args.quoted);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultF32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultF32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultF64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultF64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultVARSIZEDValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<NullableDefaultVARSIZEDValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}
}
