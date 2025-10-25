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

#pragma once

#include <type_traits>
#include <Identifiers/NESStrongType.hpp>
#include <nautilus/tracing/TypedValueRef.hpp>
#include <nautilus/tracing/Types.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_concepts.hpp>

namespace nautilus
{

namespace tracing
{
template <NES::NESIdentifier T>
struct TypeResolver<T>
{
    [[nodiscard]] static constexpr Type to_type() { return TypeResolver<typename T::Underlying>::to_type(); }
};

}

namespace details
{
template <NES::NESIdentifier LhsS>
struct RawValueResolver<LhsS>
{
    static LhsS getRawValue(const val<LhsS>& val)
    {
        return LhsS(details::RawValueResolver<typename LhsS::Underlying>::getRawValue(val.value));
    }
};

template <typename T>
requires(NES::NESIdentifier<typename std::remove_cvref_t<T>::raw_type>)
struct StateResolver<T>
{
    template <typename U = T>
    static tracing::TypedValueRef getState(U&& value)
    {
        return StateResolver<typename std::remove_cvref_t<T>::raw_type::Underlying>::getState(value.value);
    }
};

}

/// This class is a nautilus wrapper for our NESStrongType
template <typename T, typename Tag, T Invalid, T Initial>
class val<NES::NESStrongType<T, Tag, Invalid, Initial>>
{
public:
    using Underlying = typename NES::NESStrongType<T, Tag, Invalid, Initial>::Underlying;
    using raw_type = NES::NESStrongType<T, Tag, Invalid, Initial>;

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const Underlying type) : value(type) { }

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const val<Underlying> type) : value(type) { }

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const NES::NESStrongType<T, Tag, Invalid, Initial> type) : value(type.getRawValue())
    {
    }

    explicit val<NES::NESStrongType<T, Tag, Invalid, Initial>>(nautilus::tracing::TypedValueRef typedValueRef) : value(typedValueRef) { }

    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& other) : value(other.value)
    {
    }

    val<NES::NESStrongType<T, Tag, Invalid, Initial>>& operator=(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& other)
    {
        value = other.value;
        return *this;
    }

    /// IMPORTANT: This should be used with utmost care. Only, if there is no other way to work with the strong types.
    /// In general, this method should only be used to write to a Nautilus::Record of if one calls a proxy function
    val<Underlying> convertToValue() const { return value; }

    [[nodiscard]] friend bool operator<(const val& lhs, const val& rhs) noexcept { return lhs.value < rhs.value; }

    [[nodiscard]] friend bool operator<=(const val& lhs, const val& rhs) noexcept { return lhs.value <= rhs.value; }

    [[nodiscard]] friend bool operator>(const val& lhs, const val& rhs) noexcept { return lhs.value > rhs.value; }

    [[nodiscard]] friend bool operator>=(const val& lhs, const val& rhs) noexcept { return lhs.value >= rhs.value; }

    [[nodiscard]] friend bool operator==(const val& lhs, const val& rhs) noexcept { return lhs.value == rhs.value; }

    val<Underlying> value;
};
}
