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

///NOLINTBEGIN(misc-include-cleaner)
#include <cstddef>
#include <utility>
#include <Util/Reflection/OptionalReflection.hpp>
#include <Util/Reflection/PairReflection.hpp>
#include <Util/Reflection/ReflectionCore.hpp>
#include <Util/Reflection/UnorderedMapReflection.hpp>
#include <Util/Reflection/VariantReflection.hpp>
#include <Util/Reflection/VectorReflection.hpp>
#include <Util/ReflectionFwd.hpp>
#include <rfl/Generic.hpp>
#include <rfl/Tuple.hpp>
#include <rfl/named_tuple_t.hpp>

///NOLINTEND(misc-include-cleaner)

namespace NES
{

/// This header provides the complete reflection system including:
/// - Core reflection functionality (ReflectionCore.hpp)
/// - Reflector/Unreflector specializations for standard library types:
///   - std::vector
///   - std::optional
///   - std::pair
///   - std::variant
///   - std::unordered_map
///
/// For forward declarations only, use ReflectionFwd.hpp
/// For core functionality without container specializations, use ReflectionCore.hpp

namespace detail
{
/// Helper to reflect a single field at compile-time
/// Must be defined after all reflect() overloads so they're visible during instantiation
template <typename T, size_t Index>
auto reflect_field_at_index(const T& data)
{
    using NamedTupleType = rfl::named_tuple_t<T>;
    using FieldType = rfl::tuple_element_t<Index, typename NamedTupleType::Fields>;
    const auto fieldName = typename FieldType::Name().str();

    /// Get reference to the field using rfl's field introspection
    auto view = rfl::to_view(data);
    const auto& fieldValue = rfl::get<Index>(view);

    return std::make_pair(fieldName, reflect(*fieldValue));
}

/// Reflect an aggregate type field-by-field to build a Generic::Object
/// Must be defined after all reflect() overloads so they're visible during instantiation
template <typename T>
Reflected reflect_aggregate_impl(const T& data)
{
    using NamedTupleType = rfl::named_tuple_t<T>;
    constexpr size_t numFields = NamedTupleType::size();

    rfl::Generic::Object obj;

    /// Reflect each field and add to the object
    [&]<size_t... Is>(std::index_sequence<Is...>)
    {
        ((obj[reflect_field_at_index<T, Is>(data).first] = *reflect_field_at_index<T, Is>(data).second), ...);
    }(std::make_index_sequence<numFields>{});

    return Reflected{rfl::Generic::Object{std::move(obj)}};
}
}

}
