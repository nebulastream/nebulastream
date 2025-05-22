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

#include <cstdint>
#include <type_traits>
#include <Time/Timestamp.hpp>
#include <nautilus/tracing/TypedValueRef.hpp>
#include <nautilus/tracing/Types.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_concepts.hpp>

namespace nautilus
{
namespace tracing
{
template <typename T>
requires(std::is_base_of_v<NES::Runtime::Timestamp, T>)
struct TypeResolver<T>
{
    [[nodiscard]] static constexpr Type to_type() { return TypeResolver<typename T::Underlying>::to_type(); }
};

}

namespace details
{
template <typename LHS>
requires(std::is_base_of_v<NES::Runtime::Timestamp, LHS>)
struct RawValueResolver<LHS>
{
    static LHS getRawValue(const val<LHS>& val) { return LHS(details::RawValueResolver<typename LHS::Underlying>::getRawValue(val.value)); }
};

template <typename T>
requires(std::is_base_of_v<val<NES::Runtime::Timestamp>, std::remove_cvref_t<T>>)
struct StateResolver<T>
{
    template <typename U = T>
    static tracing::TypedValueRef getState(U&& value)
    {
        return StateResolver<typename std::remove_cvref_t<T>::Underlying>::getState(value.value);
    }
};

}

template <>
class val<NES::Runtime::Timestamp>
{
public:
    using Underlying = uint64_t;

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::Runtime::Timestamp>(const Underlying timestamp) : value(timestamp) { }

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::Runtime::Timestamp>(const val<Underlying>& timestamp) : value(timestamp) { }

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::Runtime::Timestamp>(const NES::Runtime::Timestamp timestamp) : value(timestamp.getRawValue()) { }

    explicit val<NES::Runtime::Timestamp>(nautilus::tracing::TypedValueRef typedValueRef) : value(typedValueRef) { }

    val<NES::Runtime::Timestamp>(const val<NES::Runtime::Timestamp>& other) = default;
    val<NES::Runtime::Timestamp>& operator=(const val<NES::Runtime::Timestamp>& other) = default;

    [[nodiscard]] friend bool operator<(const val& lhs, const val& rhs) noexcept { return lhs.value < rhs.value; }

    [[nodiscard]] friend bool operator<=(const val& lhs, const val& rhs) noexcept { return lhs.value <= rhs.value; }

    [[nodiscard]] friend bool operator>(const val& lhs, const val& rhs) noexcept { return lhs.value > rhs.value; }

    [[nodiscard]] friend bool operator>=(const val& lhs, const val& rhs) noexcept { return lhs.value >= rhs.value; }

    [[nodiscard]] friend bool operator==(const val& lhs, const val& rhs) noexcept { return lhs.value == rhs.value; }

    /// IMPORTANT: This should be used with utmost care. Only, if there is no other way to work with the strong types.
    /// In general, this method should only be used to write to a Nautilus::Record of if one calls a proxy function
    [[nodiscard]] val<Underlying> convertToValue() const { return value; }

    val<uint64_t> value;
};

}
