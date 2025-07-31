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
#include <variant>
#include <nameof.hpp>

namespace NES
{
template <template <typename> typename Container, typename... Args>
using VariantContainer = std::variant<Container<Args>...>;

namespace detail
{
template <template <typename> typename Container, typename>
struct VariantContainerFromImpl;

template <template <typename> typename Container, typename... Args>
struct VariantContainerFromImpl<Container, std::variant<Args...>>
{
    using type = VariantContainer<Container, Args...>;
};
}

template <template <typename> typename Container, typename Variant>
using VariantContainerFrom = typename detail::VariantContainerFromImpl<Container, Variant>::type;

/// checked, segfaulting access to a variant
template <typename T, typename Variant>
T get(Variant variant)
{
    PRECONDITION(std::holds_alternative<T>(variant), "Variant does not hold type {}", NAMEOF_TYPE(T));
    return std::get<T>(variant);
}
}
