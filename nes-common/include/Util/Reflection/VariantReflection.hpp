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
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <Util/Reflection/ReflectionCore.hpp>
#include <rfl/Generic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Reflector/Unreflector for std::variant<Ts...>
/// Serialized as an object {"index": N, "value": <reflected_value>} where index indicates which alternative is active
template <typename... Ts>
struct Reflector<std::variant<Ts...>>
{
    Reflected operator()(const std::variant<Ts...>& data) const
    {
        rfl::Generic::Object obj;
        obj["index"] = rfl::Generic{static_cast<int64_t>(data.index())};
        std::visit([&obj](const auto& value) { obj["value"] = *reflect(value); }, data);
        return Reflected{rfl::Generic::Object{std::move(obj)}};
    }
};

template <typename... Ts>
struct Unreflector<std::variant<Ts...>>
{
    std::variant<Ts...> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return std::visit(
            [&context, this](const auto& value) -> std::variant<Ts...>
            {
                using ValueType = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<ValueType, rfl::Generic::Object>)
                {
                    auto indexOpt = value.get("index");
                    if (!indexOpt.has_value())
                    {
                        throw CannotDeserialize("Missing 'index' field in variant object");
                    }

                    size_t index = std::visit(
                        [](const auto& idx) -> size_t
                        {
                            using IdxType = std::decay_t<decltype(idx)>;
                            if constexpr (std::is_same_v<IdxType, int64_t>)
                            {
                                return static_cast<size_t>(idx);
                            }
                            throw CannotDeserialize("Expected int64_t for variant index");
                        },
                        indexOpt.value().variant());

                    auto valueOpt = value.get("value");
                    if (!valueOpt.has_value())
                    {
                        throw CannotDeserialize("Missing 'value' field in variant object");
                    }

                    return deserialize_variant_at_index<0>(index, Reflected{valueOpt.value()}, context);
                }
                throw CannotDeserialize("Expected object for std::variant");
            },
            data->variant());
    }

private:
    template <size_t Index>
    std::variant<Ts...> deserialize_variant_at_index(size_t targetIndex, const Reflected& data, const ReflectionContext& context) const
    {
        if constexpr (Index >= sizeof...(Ts))
        {
            throw CannotDeserialize("Variant index {} out of range", targetIndex);
        }
        else
        {
            if (Index == targetIndex)
            {
                using CurrentType = std::tuple_element_t<Index, std::tuple<Ts...>>;
                return context.unreflect<CurrentType>(data);
            }
            return deserialize_variant_at_index<Index + 1>(targetIndex, data, context);
        }
    }
};

}
