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
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
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

struct Float32Bits
{
    uint32_t bits;
};

struct Float64Bits
{
    uint64_t bits;
};

using WirePayload = std::variant<std::monostate,
                                 bool,
                                 char,
                                 uint8_t,
                                 uint16_t,
                                 uint32_t,
                                 uint64_t,
                                 int8_t,
                                 int16_t,
                                 int32_t,
                                 int64_t,
                                 Float32Bits,
                                 Float64Bits,
                                 std::vector<uint8_t>>;

struct WireRuntimeValue
{
    DataType type;
    WirePayload payload;
};

template <typename T>
constexpr DataType::Type wireType()
{
    if constexpr (std::is_same_v<T, std::monostate>)
    {
        return DataType::Type::UNDEFINED;
    }
    else if constexpr (std::is_same_v<T, Float32Bits>)
    {
        return DataType::Type::FLOAT32;
    }
    else if constexpr (std::is_same_v<T, Float64Bits>)
    {
        return DataType::Type::FLOAT64;
    }
    else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
    {
        return DataType::Type::VARSIZED;
    }
    else
    {
        return RuntimeTypeTraits<T>::type;
    }
}
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
    const auto value = tryGetUnsignedInteger();
    PRECONDITION(value.has_value(), "Expected a non-negative integer runtime value");
    return *value;
}

std::optional<uint64_t> RuntimeValueView::tryGetUnsignedInteger() const
{
    return visitRuntimeType(type.type,
                            [this]<typename T>() -> std::optional<uint64_t>
                            {
                                if constexpr (std::integral<T> && !std::same_as<T, bool> && !std::same_as<T, char>)
                                {
                                    const auto value = get<T>();
                                    if constexpr (std::is_signed_v<T>)
                                    {
                                        if (value < 0)
                                        {
                                            return std::nullopt;
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
    return visitRuntimeType(type.type,
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

std::expected<RuntimeValue, Exception> RuntimeValue::fromLiteral(const DataType type, const std::string_view literal)
{
    return visitRuntimeType(type.type,
                            [type, literal]<typename T>() -> std::expected<RuntimeValue, Exception>
                            {
                                if constexpr (std::same_as<T, std::string_view>)
                                {
                                    return RuntimeValue{type, std::string{literal}};
                                }
                                else
                                {
                                    if (const auto parsed = from_chars<T>(literal))
                                    {
                                        return RuntimeValue{type, *parsed};
                                    }
                                    return std::unexpected(InvalidLiteral("'{}' is not a valid constant of type {}", literal, type));
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
    WirePayload payload = std::visit(
        []<typename T>(const T& native) -> WirePayload
        {
            if constexpr (std::same_as<T, float>)
            {
                return Float32Bits{std::bit_cast<uint32_t>(native)};
            }
            else if constexpr (std::same_as<T, double>)
            {
                return Float64Bits{std::bit_cast<uint64_t>(native)};
            }
            else if constexpr (std::same_as<T, std::string>)
            {
                return std::vector<uint8_t>{native.begin(), native.end()};
            }
            else
            {
                return native;
            }
        },
        value.payload);
    return context.reflect(WireRuntimeValue{value.type, std::move(payload)});
}

RuntimeValue Unreflector<RuntimeValue>::operator()(const Reflected& value, const ReflectionContext& context) const
{
    const auto wire = context.unreflect<WireRuntimeValue>(value);
    if (wire.type.type == DataType::Type::UNDEFINED)
    {
        throw CannotDeserialize("UNDEFINED has no runtime value representation");
    }
    RuntimeValuePayload payload = std::visit(
        [&wire](const auto& native) -> RuntimeValuePayload
        {
            using T = std::decay_t<decltype(native)>;
            if constexpr (!std::same_as<T, std::monostate>)
            {
                if (wireType<T>() != wire.type.type)
                {
                    throw CannotDeserialize("Native payload does not match runtime type {}", wire.type);
                }
            }
            if constexpr (std::same_as<T, std::monostate>)
            {
                if (!wire.type.nullable)
                {
                    throw CannotDeserialize("Cannot deserialize null for non-nullable {}", wire.type);
                }
                return std::monostate{};
            }
            else if constexpr (std::same_as<T, Float32Bits>)
            {
                return std::bit_cast<float>(native.bits);
            }
            else if constexpr (std::same_as<T, Float64Bits>)
            {
                return std::bit_cast<double>(native.bits);
            }
            else if constexpr (std::same_as<T, std::vector<uint8_t>>)
            {
                return std::string{native.begin(), native.end()};
            }
            else
            {
                return native;
            }
        },
        wire.payload);
    return RuntimeValue{wire.type, std::move(payload)};
}

}
