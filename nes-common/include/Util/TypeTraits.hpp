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
#include <optional>
#include <type_traits>

namespace NES
{

template <typename...>
inline constexpr auto UniqueTypes = std::true_type{};

template <typename T, typename... Rest>
inline constexpr auto UniqueTypes<T, Rest...> = std::bool_constant<(!std::is_same_v<T, Rest> && ...) && UniqueTypes<Rest...>>{};

template <typename... Ts>
inline constexpr auto UniqueTypesIgnoringCVRef = UniqueTypes<std::remove_cvref_t<Ts>...>;

template <typename T, typename Enable = void>
struct IsOptional : std::false_type
{
};

template <typename T>
struct IsOptional<std::optional<T>> : std::true_type
{
};

template <typename T>
concept Optional = IsOptional<T>::value;
}
