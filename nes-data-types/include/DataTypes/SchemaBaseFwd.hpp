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
#include <concepts>
#include <cstddef>
#include <functional>
#include <type_traits>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <fmt/base.h>

namespace NES
{

namespace detail
{
template <typename T, typename R>
concept same_as_ignoring_cvref = std::same_as<std::remove_cvref_t<T>, std::remove_cvref_t<R>>;

}

template <typename T, size_t IdListExtent>
concept FieldConcept = requires(T field) {
    { field.getFullyQualifiedName() } -> detail::same_as_ignoring_cvref<IdentifierListBase<IdListExtent>>;
    { field.getDataType() } -> std::convertible_to<DataType>;
    { std::hash<T>{}(field) };
} && std::equality_comparable<T> && std::copy_constructible<T> && fmt::formattable<T>;

struct OrderType
{
    bool ordered;
};

static constexpr OrderType Ordered{true};
static constexpr OrderType Unordered{false};

template <typename FieldType, OrderType IsOrdered>
class Schema;
}
