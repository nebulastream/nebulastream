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
#include <Runtime/VariableSizedAccess.hpp>
#include <nautilus/tracing/TypedValueRef.hpp>
#include <nautilus/tracing/Types.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_concepts.hpp>

namespace nautilus
{
namespace tracing
{
template <typename T>
requires(std::is_base_of_v<NES::VariableSizedAccess, T>)
struct TypeResolver<T>
{
    [[nodiscard]] static constexpr Type to_type() { return TypeResolver<typename T::CombinedIndex>::to_type(); }
};

}

namespace details
{
template <typename LHS>
requires(std::is_base_of_v<NES::VariableSizedAccess, LHS>)
struct RawValueResolver<LHS>
{
    static LHS getRawValue(const val<LHS>& val)
    {
        return LHS{details::RawValueResolver<typename LHS::CombinedIndex>::getRawValue(val.variableSizedAccess)};
    }
};

template <typename T>
requires(std::is_base_of_v<val<NES::VariableSizedAccess>, std::remove_cvref_t<T>>)
struct StateResolver<T>
{
    template <typename U = T>
    static tracing::TypedValueRef getState(U&& value)
    {
        return StateResolver<typename std::remove_cvref_t<T>::Underlying>::getState(value.variableSizedAccess);
    }
};

}

/// We are specializing the nautilus::val<> implementation so that we can use nautilus::val<VariableSizedAccess>
template <>
class val<NES::VariableSizedAccess>
{
public:
    /// Friend declarations for the specializations
    template <typename LHS>
    friend struct details::RawValueResolver;

    template <typename T>
    friend struct details::StateResolver;

    using Underlying = NES::VariableSizedAccess::CombinedIndex;

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    explicit val(const Underlying variableSizedAccess) : variableSizedAccess(variableSizedAccess) { }

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    explicit val(const val<Underlying>& variableSizedAccess) : variableSizedAccess(variableSizedAccess) { }

    /// ReSharper disable once CppNonExplicitConvertingConstructor
    explicit val(const NES::VariableSizedAccess variableSizedAccess) : variableSizedAccess(variableSizedAccess.getCombinedIdxOffset()) { }

    explicit val(tracing::TypedValueRef typedValueRef) : variableSizedAccess(typedValueRef) { }

    val(const val& other) = default;
    val& operator=(const val& other) = default;

    /// IMPORTANT: This should be used with utmost care. Only, if there is no other way to work with the strong types.
    /// In general, this method should only be used to write to a Nautilus::Record of if one calls a proxy function
    [[nodiscard]] val<Underlying> convertToValue() const { return variableSizedAccess; }

private:
    val<Underlying> variableSizedAccess;
};

}
