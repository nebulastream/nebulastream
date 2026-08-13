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
#include <array>
#include <chrono>
#include <cstddef>
#include <map>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/TypeTraits.hpp>
#include <fmt/chrono.h> /// NOLINT(misc-include-cleaner) -- required for fmt::format of std::chrono::time_point
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <rfl/Generic.hpp>
#include <rfl/Object.hpp>
#include <rfl/to_generic.hpp>
#include <InputFormatterConfigRegistry.hpp>
#include <InputFormatterDescriptor.hpp>

/// Local converters that translate frontend statement-result values into rfl::Generic for JSON output.
/// We deliberately do NOT specialize rfl::Reflector for shared types like Schema, DataType, NESStrongType, etc.,
/// because those types already have NES::Reflector specializations that the rfl::Reflector partial-specialization
/// in <Util/Reflection.hpp> picks up. Specializing rfl::Reflector again here would violate the One Definition Rule:
/// the same rfl::Reflector specialization would resolve to a different ReflType depending on which translation unit
/// included this header, which is undefined behaviour and crashes at runtime.

namespace NES::detail
{

inline rfl::Generic toFrontendGeneric(const DataType& dataType, const ReflectionContext& context)
{
    return context.reflect(std::string(magic_enum::enum_name(dataType.type)));
}

/// Reflector<Identifier> retains SQL quotes for case-sensitive identifiers.
/// The historical frontend JSON shape is the canonical string without quote metadata — override here.
inline rfl::Generic toFrontendGeneric(const Identifier& identifier, const ReflectionContext& context)
{
    return context.reflect(identifier.asCanonicalString());
}

inline rfl::Generic toFrontendGeneric(const UnqualifiedUnboundField& field, const ReflectionContext& context)
{
    rfl::Object<rfl::Generic> obj;
    obj["name"] = context.reflect(field.getFullyQualifiedName());
    obj["type"] = toFrontendGeneric(field.getDataType(), context);
    return obj;
}

/// Historical shape: an outer array wrapping the array of fields.
inline rfl::Generic toFrontendGeneric(const Schema<UnqualifiedUnboundField, Ordered>& schema, const ReflectionContext& context)
{
    rfl::Generic::Array fields;
    fields.reserve(schema.size());
    for (const auto& field : schema)
    {
        fields.emplace_back(toFrontendGeneric(field, context));
    }
    return fields;
}

/// Flat object: the formatter type under "type", then each member of the formatter-defined config
/// struct (serialized via the formatter's InputFormatterConfigRegistry entry — the only place that
/// knows the concrete type behind the descriptor's std::any). Formats without a config (NATIVE)
/// yield only the type.
inline rfl::Generic toFrontendGeneric(const InputFormatterDescriptor& descriptor, const ReflectionContext& context)
{
    rfl::Object<rfl::Generic> obj;
    obj["type"] = context.reflect(descriptor.getInputFormatterType());
    if (descriptor.getConfig().getUnderlying().has_value())
    {
        if (const auto configEntry = InputFormatterConfigRegistry::instance().find(descriptor.getInputFormatterType().asCanonicalString()))
        {
            const rfl::Generic reflectedConfig = configEntry->reflect(descriptor.getConfig(), context);
            if (const auto configObject = reflectedConfig.to_object())
            {
                for (const auto& [key, value] : *configObject)
                {
                    obj[key] = value;
                }
            }
        }
    }
    return obj;
}

template <typename Clock, typename Duration>
rfl::Generic toFrontendGeneric(const std::chrono::time_point<Clock, Duration>& timepoint, const ReflectionContext& context)
{
    rfl::Object<rfl::Generic> obj;
    obj["since_epoch"] = context.reflect(std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count());
    obj["unit"] = context.reflect(std::string("microseconds"));
    obj["formatted"] = context.reflect(fmt::format("{}", timepoint));
    return obj;
}

/// Enums serialize as their name (e.g. `QueryStatus::Running` -> "Running") rather than the
/// underlying integer value the default reflection chain would emit.
template <typename T>
requires std::is_enum_v<T>
rfl::Generic toFrontendGeneric(const T& value, const ReflectionContext& context)
{
    return context.reflect(std::string(magic_enum::enum_name(value)));
}

/// Fallback for any type without a specific overload. Routes through the existing reflection chain
/// (rfl::to_generic + NES::Reflector partial specialization), which already handles primitives,
/// optionals, variants, maps, and NES strong-typed identifiers in a way compatible with the frontend output.
template <typename T>
rfl::Generic toFrontendGeneric(const T& value, const ReflectionContext& context)
{
    return context.reflect(value);
}

template <typename T>
void appendField(rfl::Object<rfl::Generic>& target, std::string_view fieldName, const T& field, const ReflectionContext& context)
{
    if constexpr (Optional<T>)
    {
        if (field.has_value())
        {
            target[std::string(fieldName)] = toFrontendGeneric(*field, context);
        }
    }
    else
    {
        target[std::string(fieldName)] = toFrontendGeneric(field, context);
    }
}

}

namespace NES
{

/// Converts a (column-names, rows-of-tuples) pair as produced by StatementOutputAssembler into a
/// JSON array of objects. Optional fields with no value are omitted.
template <size_t N, typename... Ts>
rfl::Generic::Array rowsToJsonArray(
    const std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>& assembled, const ReflectionContext& context)
{
    rfl::Generic::Array rows;
    rows.reserve(assembled.second.size());
    for (const auto& row : assembled.second)
    {
        rfl::Object<rfl::Generic> obj;
        [&]<size_t... Is>(std::index_sequence<Is...>)
        { (detail::appendField(obj, assembled.first[Is], std::get<Is>(row), context), ...); }(std::index_sequence_for<Ts...>{});
        rows.emplace_back(std::move(obj));
    }
    return rows;
}

}
