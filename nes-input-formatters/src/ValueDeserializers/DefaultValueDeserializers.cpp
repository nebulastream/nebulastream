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
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>
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

template <typename T, bool Nullable, bool Quoted>
void deserializeFixedSized(
    const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues, DeserializedResult<T>* result)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    result->isNull = false;
    result->isNull = false;

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullProxy(fieldAddress, fieldSize, nullValues))
        {
            result->isNull = true;
            result->value = T{0};
            return;
        }
    }

    /// Remove the quotes from the address, if quoted.
    const int8_t* trueFieldAddress = Quoted ? fieldAddress + 1 : fieldAddress;
    const uint64_t trueFieldSize = Quoted ? fieldSize - 2 : fieldSize;

    try
    {
        const std::string fieldAsString{trueFieldAddress, trueFieldAddress + trueFieldSize};
        const auto trimmedFieldAsString = trimWhiteSpaces(fieldAsString);
        result->value = NES::from_chars_with_exception<T>(trimmedFieldAsString);
    }
    catch (const Exception& ex)
    {
        /// If the field is nullable, we return a null value, otherwise we throw an exception
        if constexpr (not Nullable)
        {
            throw;
        }
        result->isNull = true;
        result->value = T{0};
    }
}

struct TruncateSpacesResult
{
    const int8_t* ptr;
    uint64_t size;
};

/// Obtains the pointer and size of a field.
/// Returns a pointer and the size of the field without the trailing whitespaces
void truncateTrailingSpaces(const int8_t* ptr, const uint64_t size, TruncateSpacesResult* result)
{
    std::string_view uncutString{reinterpret_cast<const char*>(ptr), size};
    if (const auto lastNonWs = uncutString.find_last_not_of(" \t\n\r"); lastNonWs != std::string_view::npos)
    {
        uncutString = uncutString.substr(0, lastNonWs + 1);
    }
    result->ptr = reinterpret_cast<const int8_t*>(uncutString.data());
    result->size = uncutString.size();
}

}

namespace NES
{

VarVal DefaultBOOLValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<bool>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<bool, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<bool> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<bool>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultBOOLValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<bool>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<bool, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<bool> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<bool>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<bool>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<char> nautilusValue{char{0}};
    nautilus::val<DefaultValueDeserializer::DeserializedResult<char>> deserializedResult;
    if (quoted)
    {
        nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, false, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues},
            &deserializedResult);
        nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<char>::value);
    }
    else
    {
        nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, false, false>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues},
            &deserializedResult);
        nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<char>::value);
    }
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<char> nautilusValue{char{0}};
    nautilus::val<bool> isNull{false};
    nautilus::val<DefaultValueDeserializer::DeserializedResult<char>> deserializedResult;
    if (quoted)
    {
        nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, true, true>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues},
            &deserializedResult);
        nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<char>::value);
        isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<char>::isNull);
    }
    else
    {
        nautilus::invoke(
            DefaultValueDeserializer::deserializeFixedSized<char, true, false>,
            fieldAddress,
            fieldSize,
            nautilus::val<const std::vector<std::string>*>{&nullValues},
            &deserializedResult);
        nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<char>::value);
        isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<char>::isNull);
    }
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultF32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<float>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<float, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<float> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<float>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultF32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<float>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<float, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<float> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<float>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<float>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultF64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<double>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<double, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<double> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<double>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultF64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<double>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<double, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<double> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<double>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<double>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int8_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int8_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int8_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int8_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int8_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int8_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int8_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int8_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int8_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int16_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int16_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int16_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int16_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int16_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int16_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int16_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int16_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int16_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int32_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int32_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int32_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int32_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int32_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int32_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int32_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int32_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int32_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int64_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int64_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int64_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int64_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<int64_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<int64_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<int64_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int64_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<int64_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint8_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint8_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint8_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint8_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT8ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint8_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint8_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint8_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint8_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint8_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint16_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint16_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint16_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint16_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT16ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint16_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint16_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint16_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint16_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint16_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint32_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint32_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint32_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint32_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT32ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint32_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint32_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint32_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint32_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint32_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultUINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint64_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint64_t, false, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint64_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint64_t>::value);
    return VarVal{nautilusValue, false, false};
}

VarVal NullableDefaultUINT64ValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<DefaultValueDeserializer::DeserializedResult<uint64_t>> deserializedResult;
    nautilus::invoke(
        DefaultValueDeserializer::deserializeFixedSized<uint64_t, true, false>,
        fieldAddress,
        fieldSize,
        nautilus::val<const std::vector<std::string>*>{&nullValues},
        &deserializedResult);
    const nautilus::val<uint64_t> nautilusValue = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint64_t>::value);
    const nautilus::val<bool> isNull = deserializedResult.get(&DefaultValueDeserializer::DeserializedResult<uint64_t>::isNull);
    return VarVal{nautilusValue, true, isNull};
}

VarVal DefaultVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef&) const
{
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    if (hasTrailingSpaces)
    {
        /// Cut off trailing whitespace
        nautilus::val<DefaultValueDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultValueDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultValueDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultValueDeserializer::TruncateSpacesResult::size);
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
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef&) const
{
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    nautilus::val<bool> isNull = false;
    if (hasTrailingSpaces)
    {
        /// Cut off trailing whitespace
        nautilus::val<DefaultValueDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultValueDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultValueDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultValueDeserializer::TruncateSpacesResult::size);
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

ValueDeserializerRegistryReturnType DefaultBOOLValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultBOOLValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultCHARValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultCHARValueDeserializer>(args.quoted);
}

ValueDeserializerRegistryReturnType DefaultF32ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultF32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultF64ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultF64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultINT8ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultINT16ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultINT32ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultINT64ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultUINT8ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultUINT16ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultUINT32ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultUINT64ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultUINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType DefaultVARSIZEDValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultVARSIZEDValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}

ValueDeserializerRegistryReturnType NullableDefaultBOOLValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultBOOLValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultCHARValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<NullableDefaultCHARValueDeserializer>(args.quoted);
}

ValueDeserializerRegistryReturnType NullableDefaultF32ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultF32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultF64ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultF64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultINT8ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultINT16ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultINT32ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultINT64ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultUINT8ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT8ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultUINT16ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT16ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultUINT32ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT32ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultUINT64ValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableDefaultUINT64ValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableDefaultVARSIZEDValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<NullableDefaultVARSIZEDValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}
}
