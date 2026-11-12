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

#include <DataTypes/RuntimeValue.hpp>

#include <bit>
#include <concepts>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
#ifndef NO_ASSERT
template <typename Payload>
void validatePayload(const DataType type, const Payload& payload)
{
    PRECONDITION(type.type != DataType::Type::UNDEFINED, "UNDEFINED has no runtime value representation");
    std::visit(
        [&]<typename T>(const T&)
        {
            if constexpr (std::is_same_v<T, std::monostate>)
            {
                PRECONDITION(type.nullable, "Cannot construct null for non-nullable {}", type);
            }
            else
            {
                PRECONDITION(RuntimeTypeTraits<T>::type == type.type, "Native payload does not match runtime type {}", type);
            }
        },
        payload);
}
#endif
}

RuntimeValueView::RuntimeValueView(const DataType type, RuntimeValueViewPayload payload) : type(type), payload(std::move(payload))
{
    IF_PRECONDITION(validatePayload(type, this->payload);)
}

DataType RuntimeValueView::getDataType() const
{
    return type;
}

bool RuntimeValueView::isNull() const
{
    return std::holds_alternative<std::monostate>(payload);
}

std::span<const std::byte> RuntimeValueView::bytes() const
{
    return std::as_bytes(std::span{get<std::string_view>()});
}

uint64_t RuntimeValueView::getUnsignedInteger() const
{
    return visitRuntimeType(
        type.type,
        [this]<typename T>() -> uint64_t
        {
            if constexpr (std::integral<T> && !std::same_as<T, bool> && !std::same_as<T, char>)
            {
                const auto value = get<T>();
                if constexpr (std::is_signed_v<T>)
                {
                    if (value < 0)
                    {
                        throw DifferentFieldTypeExpected("Expected a non-negative integer, got {}", value);
                    }
                }
                return static_cast<uint64_t>(value);
            }
            else
            {
                PRECONDITION(false, "Expected an integer runtime value, got {}", type);
                std::unreachable();
            }
        });
}

RuntimeValue RuntimeValueView::toOwned() const
{
    if (isNull())
    {
        return RuntimeValue::null(type);
    }
    return visitRuntimeType(
        type.type,
        [this]<typename T>() -> RuntimeValue
        {
            if constexpr (std::same_as<T, std::string_view>)
            {
                return RuntimeValue{type, std::string{get<T>()}};
            }
            else
            {
                return RuntimeValue{type, get<T>()};
            }
        });
}

RuntimeValue::RuntimeValue(const DataType type, RuntimeValuePayload payload) : type(type), payload(std::move(payload))
{
    IF_PRECONDITION(validatePayload(type, this->payload);)
}

RuntimeValue RuntimeValue::null(const DataType type)
{
    return RuntimeValue{type, std::monostate{}};
}

RuntimeValue RuntimeValue::fromLiteral(const DataType type, const std::string_view literal)
{
    return visitRuntimeType(
        type.type,
        [type, literal]<typename T>() -> RuntimeValue
        {
            if constexpr (std::same_as<T, std::string_view>)
            {
                return RuntimeValue{type, std::string{literal}};
            }
            else
            {
                return RuntimeValue{type, from_chars_with_exception<T>(literal)};
            }
        });
}

DataType RuntimeValue::getDataType() const
{
    return type;
}

bool RuntimeValue::isNull() const
{
    return std::holds_alternative<std::monostate>(payload);
}

RuntimeValueView RuntimeValue::view() const&
{
    return std::visit(
        [this]<typename T>(const T& value) -> RuntimeValueView
        {
            if constexpr (std::same_as<T, std::string>)
            {
                return RuntimeValueView{type, std::string_view{value}};
            }
            else
            {
                return RuntimeValueView{type, value};
            }
        },
        payload);
}

Reflected Reflector<RuntimeValue>::operator()(const RuntimeValue& value, const ReflectionContext& context) const
{
    Reflected payload;
    if (!value.isNull())
    {
        payload = visitRuntimeType(
            value.getDataType().type,
            [&]<typename T>() -> Reflected
            {
                if constexpr (std::same_as<T, std::string_view>)
                {
                    /// VARSIZED is arbitrary bytes; JSON strings require UTF-8. Do not silently change or reject binary constants.
                    const auto bytes = value.view().get<T>();
                    return context.reflect(std::vector<uint8_t>{bytes.begin(), bytes.end()});
                }
                else if constexpr (std::floating_point<T>)
                {
                    /// Preserve signed zero and every NaN payload; JSON numbers cannot represent all floating-point values.
                    using Bits = std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>;
                    return context.reflect(std::bit_cast<Bits>(value.view().get<T>()));
                }
                else
                {
                    return context.reflect(value.view().get<T>());
                }
            });
    }
    return context.reflect(std::pair{value.getDataType(), payload});
}

RuntimeValue Unreflector<RuntimeValue>::operator()(const Reflected& value, const ReflectionContext& context) const
{
    const auto [type, payload] = context.unreflect<std::pair<DataType, Reflected>>(value);
    if (payload.isEmpty())
    {
        return RuntimeValue::null(type);
    }
    return visitRuntimeType(
        type.type,
        [&]<typename T>() -> RuntimeValue
        {
            if constexpr (std::same_as<T, std::string_view>)
            {
                const auto bytes = context.unreflect<std::vector<uint8_t>>(payload);
                return RuntimeValue{type, std::string{bytes.begin(), bytes.end()}};
            }
            else if constexpr (std::floating_point<T>)
            {
                using Bits = std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>;
                return RuntimeValue{type, std::bit_cast<T>(context.unreflect<Bits>(payload))};
            }
            else
            {
                return RuntimeValue{type, context.unreflect<T>(payload)};
            }
        });
}

}
