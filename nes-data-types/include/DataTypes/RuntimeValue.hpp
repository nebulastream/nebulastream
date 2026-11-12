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
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// The native scalar types supported by DataType. VARSIZED is a byte sequence, not necessarily text.
/// Keep the native/SQL mapping here; runtime consumers need neither Nautilus nor tuple buffers.
template <typename T>
struct RuntimeTypeTraits;

#define NES_RUNTIME_TYPE(CPP_TYPE, SQL_TYPE) \
    template <> \
    struct RuntimeTypeTraits<CPP_TYPE> \
    { \
        static constexpr DataType::Type type = DataType::Type::SQL_TYPE; \
    }

NES_RUNTIME_TYPE(bool, BOOLEAN);
NES_RUNTIME_TYPE(char, CHAR);
NES_RUNTIME_TYPE(uint8_t, UINT8);
NES_RUNTIME_TYPE(uint16_t, UINT16);
NES_RUNTIME_TYPE(uint32_t, UINT32);
NES_RUNTIME_TYPE(uint64_t, UINT64);
NES_RUNTIME_TYPE(int8_t, INT8);
NES_RUNTIME_TYPE(int16_t, INT16);
NES_RUNTIME_TYPE(int32_t, INT32);
NES_RUNTIME_TYPE(int64_t, INT64);
NES_RUNTIME_TYPE(float, FLOAT32);
NES_RUNTIME_TYPE(double, FLOAT64);
NES_RUNTIME_TYPE(std::string, VARSIZED);
NES_RUNTIME_TYPE(std::string_view, VARSIZED);
#undef NES_RUNTIME_TYPE

template <typename T>
concept RuntimeScalar = requires { RuntimeTypeTraits<std::remove_cvref_t<T>>::type; };

/// Dispatches a SQL type to its exact native representation. Undefined values cannot cross a runtime boundary.
template <typename Visitor>
decltype(auto) visitRuntimeType(const DataType::Type type, Visitor&& visitor)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return std::forward<Visitor>(visitor).template operator()<bool>();
        case DataType::Type::CHAR:
            return std::forward<Visitor>(visitor).template operator()<char>();
        case DataType::Type::UINT8:
            return std::forward<Visitor>(visitor).template operator()<uint8_t>();
        case DataType::Type::UINT16:
            return std::forward<Visitor>(visitor).template operator()<uint16_t>();
        case DataType::Type::UINT32:
            return std::forward<Visitor>(visitor).template operator()<uint32_t>();
        case DataType::Type::UINT64:
            return std::forward<Visitor>(visitor).template operator()<uint64_t>();
        case DataType::Type::INT8:
            return std::forward<Visitor>(visitor).template operator()<int8_t>();
        case DataType::Type::INT16:
            return std::forward<Visitor>(visitor).template operator()<int16_t>();
        case DataType::Type::INT32:
            return std::forward<Visitor>(visitor).template operator()<int32_t>();
        case DataType::Type::INT64:
            return std::forward<Visitor>(visitor).template operator()<int64_t>();
        case DataType::Type::FLOAT32:
            return std::forward<Visitor>(visitor).template operator()<float>();
        case DataType::Type::FLOAT64:
            return std::forward<Visitor>(visitor).template operator()<double>();
        case DataType::Type::VARSIZED:
            return std::forward<Visitor>(visitor).template operator()<std::string_view>();
        case DataType::Type::UNDEFINED:
            break;
    }
    PRECONDITION(false, "Datatype {} has no runtime value representation", static_cast<int>(type));
    std::unreachable();
}

using RuntimeValuePayload = std::variant<
    std::monostate,
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
    float,
    double,
    std::string>;
using RuntimeValueViewPayload = std::variant<
    std::monostate,
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
    float,
    double,
    std::string_view>;

class RuntimeValue;

/// Read-only scalar access, independent of its storage. Fixed-size values are copied; VARSIZED borrows its bytes.
/// A view must not outlive the owning value/buffer, or survive mutation of that storage.
class RuntimeValueView
{
public:
    RuntimeValueView(DataType type, RuntimeValueViewPayload payload);

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] bool isNull() const;

    /// Exact extraction, not a numeric cast. A null value must be checked before accessing its payload.
    template <RuntimeScalar T>
    [[nodiscard]] T get() const
    requires(!std::is_same_v<T, std::string>)
    {
        PRECONDITION(!isNull(), "Cannot read the payload of a null {}", type);
        PRECONDITION(std::holds_alternative<T>(payload), "Runtime value {} does not have the requested native type", type);
        return *std::get_if<T>(&payload);
    }

    [[nodiscard]] std::span<const std::byte> bytes() const;
    /// Explicit conversion for integer options/durations; negative input is a user-data error.
    /// The caller must supply a non-null integer value (not BOOLEAN or CHAR).
    [[nodiscard]] uint64_t getUnsignedInteger() const;
    /// Copy into owned storage, preserving the exact datatype, including nullability.
    [[nodiscard]] RuntimeValue toOwned() const;

private:
    DataType type;
    RuntimeValueViewPayload payload;
};

/// An owning, typed scalar for runtime boundaries. Nullable type and null value are separate properties.
/// VARSIZED uses std::string as binary-safe owned storage, including embedded NULs.
/// Producers must supply a matching native payload and datatype; contract checks are preconditions, not conversions.
class RuntimeValue
{
public:
    template <RuntimeScalar T>
    explicit RuntimeValue(T value)
        : RuntimeValue(DataType{RuntimeTypeTraits<T>::type, DataType::NULLABLE::NOT_NULLABLE}, makePayload(std::move(value)))
    {
    }

    explicit RuntimeValue(std::string_view value)
        : RuntimeValue(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, std::string{value})
    {
    }

    RuntimeValue(DataType type, RuntimeValuePayload payload);

    [[nodiscard]] static RuntimeValue null(DataType type);
    /// Parses a logical constant once at binding time. Strings are already unquoted logical values.
    [[nodiscard]] static RuntimeValue fromLiteral(DataType type, std::string_view literal);

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] bool isNull() const;
    [[nodiscard]] RuntimeValueView view() const&;
    RuntimeValueView view() const&& = delete;

private:
    template <RuntimeScalar T>
    static RuntimeValuePayload makePayload(T value)
    {
        if constexpr (std::is_same_v<T, std::string_view>)
        {
            return std::string{value};
        }
        else
        {
            return RuntimeValuePayload{std::move(value)};
        }
    }

    DataType type;
    RuntimeValuePayload payload;
};

template <>
struct Reflector<RuntimeValue>
{
    Reflected operator()(const RuntimeValue& value, const ReflectionContext& context) const;
};

template <>
struct Unreflector<RuntimeValue>
{
    RuntimeValue operator()(const Reflected& value, const ReflectionContext& context) const;
};

}
