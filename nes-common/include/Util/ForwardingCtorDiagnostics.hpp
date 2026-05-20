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

#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

/// Diagnostic helpers for type-erased wrappers that forward constructor arguments to a wrapped
/// target type. Produces a readable static_assert when forwarding fails, distinguishing between
/// wrong arity, a single argument with the wrong type, and multiple arguments with wrong types.
///
/// `PrependedTuple` lists types the wrapper implicitly prepends before the user's args (e.g. the
/// LogicalOperator wrapper prepends a WeakLogicalOperator). Pass `std::tuple<>` if the wrapper
/// forwards args verbatim (e.g. a LogicalFunction wrapper).
///
/// Usage in a fallback ctor (only enabled when forwarding fails):
///   NES::Util::ForwardingCtor::diagnoseConstructibility<Target, std::tuple<Prepended...>, Args...>();
namespace NES::Util::ForwardingCtor
{

namespace detail
{
/// Compile-time type that converts to anything; used to probe constructor signatures. Only usable in unevaluated context.
struct AnyConvertible
{
    template <typename T>
    explicit operator T() const noexcept;
};

template <size_t>
using AnyConvertibleAt = AnyConvertible;

/// Triggered when argument at zero-based index `Pos` has the wrong type. `Pos` and the offending
/// type `Provided` are encoded in the template parameters so they appear in the compiler diagnostic.
template <size_t Pos, typename Provided>
struct ArgumentHasWrongTypeAt
{
    static_assert(
        false,
        "argument at the zero-based index encoded in this template parameter has the wrong type for the forwarding constructor (the type "
        "the caller passed is the second template parameter)");
};

/// Checks if `Target` has a constructor accepting `(Prepended..., <N args of any type>)`.
template <typename Target, typename PrependedTuple, size_t N, typename = std::make_index_sequence<N>>
struct HasArityConstructor;

template <typename Target, typename... Prepended, size_t N, size_t... Is>
struct HasArityConstructor<Target, std::tuple<Prepended...>, N, std::index_sequence<Is...>>
{
    static constexpr bool value = std::is_constructible_v<Target, Prepended..., AnyConvertibleAt<Is>...>;
};

/// Checks if replacing `ArgsTuple`'s element at index `I` with AnyConvertible makes
/// `(Target, Prepended..., Args...)` constructible.
template <
    typename Target,
    typename PrependedTuple,
    typename ArgsTuple,
    size_t I,
    typename = std::make_index_sequence<std::tuple_size_v<ArgsTuple>>>
struct ConstructibleReplacingArg;

template <typename Target, typename... Prepended, typename ArgsTuple, size_t I, size_t... Is>
struct ConstructibleReplacingArg<Target, std::tuple<Prepended...>, ArgsTuple, I, std::index_sequence<Is...>>
{
    static constexpr bool value = std::
        is_constructible_v<Target, Prepended..., std::conditional_t<Is == I, AnyConvertible, std::tuple_element_t<Is, ArgsTuple>>...>;
};

/// Returns the first argument index where replacing it with AnyConvertible fixes constructibility,
/// or `tuple_size_v<ArgsTuple>` if no single replacement is sufficient.
template <typename Target, typename PrependedTuple, typename ArgsTuple, size_t I = 0>
consteval size_t findMismatchedArg()
{
    if constexpr (I >= std::tuple_size_v<ArgsTuple>)
    {
        return std::tuple_size_v<ArgsTuple>;
    }
    else if constexpr (ConstructibleReplacingArg<Target, PrependedTuple, ArgsTuple, I>::value)
    {
        return I;
    }
    else
    {
        return findMismatchedArg<Target, PrependedTuple, ArgsTuple, I + 1>();
    }
}
}

/// Emits a readable compile-time diagnostic explaining why `Target` cannot be constructed from
/// `(Prepended..., Args...)`. Call from the body of a fallback constructor whose `requires` clause
/// guarantees the primary forwarding ctor failed to match.
template <typename Target, typename PrependedTuple, typename... Args>
constexpr void diagnoseConstructibility()
{
    constexpr bool arityOk = detail::HasArityConstructor<Target, PrependedTuple, sizeof...(Args)>::value;
    if constexpr (!arityOk)
    {
        static_assert(false, "wrong number of arguments for forwarding constructor");
    }
    else
    {
        constexpr size_t badPos = detail::findMismatchedArg<Target, PrependedTuple, std::tuple<Args...>>();
        if constexpr (badPos >= sizeof...(Args))
        {
            static_assert(false, "multiple arguments have wrong types for forwarding constructor");
        }
        else
        {
            detail::ArgumentHasWrongTypeAt<badPos, std::tuple_element_t<badPos, std::tuple<Args...>>>{};
        }
    }
}

}
