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

namespace NES
{

//Convert a variant over a set of types to a variant with a superset of types
//Uses std::visit to recast
namespace detail
{

template <typename... From>
class VariantProxyLRef
{
    std::variant<From...>& underlying;

public:
    explicit constexpr VariantProxyLRef(std::variant<From...>& underlying) : underlying(underlying) { }
    template <typename... To>
    constexpr operator std::variant<To...>() &&
    {
        return std::visit([](auto&& argument) -> std::variant<To...> { return std::forward<decltype(argument)>(argument); }, underlying);
    }
};

template <typename... From>
class VariantProxyConstantLRef
{
    const std::variant<From...>& underlying;

public:
    explicit constexpr VariantProxyConstantLRef(std::variant<From...>& underlying) : underlying(underlying) { }
    template <typename... To>
    constexpr operator std::variant<To...>() &&
    {
        return std::visit([](auto&& argument) -> std::variant<To...> { return std::forward<decltype(argument)>(argument); }, underlying);
    }
};

template <typename... From>
class VariantProxyRRef
{
    const std::variant<From...>&& underlying;

public:
    explicit constexpr VariantProxyRRef(std::variant<From...>&& underlying) : underlying(std::move(underlying)) { }
    template <typename... To>
    constexpr operator std::variant<To...>() &&
    {
        return std::visit(
            [](auto&& argument) -> std::variant<To...> { return std::forward<decltype(argument)>(argument); }, std::move(underlying));
    }
};
}
template <typename... From>
constexpr detail::VariantProxyLRef<From...> variantFromSubset(std::variant<From...>& underlying)
{
    return detail::VariantProxyLRef{underlying};
}
template <typename... From>
constexpr detail::VariantProxyConstantLRef<From...> variantFromSubset(const std::variant<From...>& underlying)
{
    return detail::VariantProxyLRef{underlying};
}

template <typename... From>
constexpr detail::VariantProxyRRef<From...> variant(std::variant<From...>&& underlying)
{
    return detail::VariantProxyRRef{std::move(underlying)};
}

template <typename T, typename... From>
constexpr std::optional<T> getOptional(std::variant<From...> variant) {
    if (std::holds_alternative<T>(variant)){
        return std::get<T>(variant);
    }
    return std::nullopt;
    }
}
