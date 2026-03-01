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

#include <any>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <variant>

#include <rfl/Generic.hpp>
#include <rfl/Tuple.hpp>
#include <rfl/from_generic.hpp>
#include <rfl/named_tuple_t.hpp>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES
{

/// Helper for static_assert that always evaluates to false
template <typename T>
inline constexpr bool always_false_v = false;

struct Reflected
{
    Reflected() : value(std::nullopt) { }

    ///NOLINTNEXTLINE(google-explicit-constructor)
    Reflected(rfl::Generic value) : value(std::move(value)) { }

    ///NOLINTNEXTLINE(google-explicit-constructor)
    operator rfl::Generic() const { return value; }

    const rfl::Generic& operator*() const { return value; }

    const rfl::Generic* operator->() const { return &value; }

    [[nodiscard]] bool isEmpty() const { return this->value.is_null(); }

private:
    rfl::Generic value;
};

template <typename T>
struct Unreflector;


/// Type has an Unreflector that takes Reflected and ReflectionContext parameters
template <typename T>
concept HasTwoParamUnreflector = requires(const Reflected& data, const class ReflectionContext& ctx) {
    { Unreflector<T>{}(data, ctx) } -> std::same_as<T>;
};

/// Type has a contextful Unreflector (constructor-based context passing)
template <typename T>
concept ContextfulUnreflector = requires(const class ReflectionContext& context, const Reflected& data) {
    typename Unreflector<T>::ContextType;
    { Unreflector<T>{std::declval<typename Unreflector<T>::ContextType>()} };
    { Unreflector<T>{std::declval<typename Unreflector<T>::ContextType>()}(data, context) } -> std::same_as<T>;
};

/// Type has any explicit Unreflector specialization
template <typename T>
concept HasExplicitUnreflector = HasTwoParamUnreflector<T> || ContextfulUnreflector<T>;

/// Type is a primitive type (arithmetic or string)
template <typename T>
concept Primitive = std::is_arithmetic_v<T> || std::is_same_v<T, std::string>;

/// Context object for deserialization that allows passing configuration or state down the call chain.
///
/// This context enables contextful unreflectors to receive additional information during deserialization.
/// Instead of calling rfl::from_generic directly (which loses context), ReflectionContext manually
/// deserializes each field through context.unreflect(), ensuring context flows through the entire tree.
///
/// The context is immutable after construction - all context values must be provided during initialization.
///
/// Example usage:
/// @code
/// struct MyUnreflector {
///     using ContextType = MyConfig;
///     MyConfig config;
///     MyType operator()(const Reflected& data, const ReflectionContext& ctx) const { /* use config */ }
/// };
///
/// ReflectionContext context(MyConfig{...}, MyOtherConfig{...});
/// auto result = context.unreflect<MyType>(reflected_data);
/// @endcode
class ReflectionContext
{
    std::unordered_map<std::type_index, std::any> context;

public:
    explicit ReflectionContext() = default;

    template <typename... Args>
    explicit ReflectionContext(Args... args) : context{(std::pair{std::type_index{typeid(Args)}, std::any{std::move(args)}}, ...)}
    {
    }

    template <typename T>
    [[nodiscard]] T unreflect(const Reflected& data) const;

    /// Overload for types with contextless Unreflector
    template <typename T>
    requires HasTwoParamUnreflector<T>
    [[nodiscard]] T unreflect(const Reflected& data) const
    {
        return Unreflector<T>{}(data, *this);
    }

    /// Overload for types with contextful Unreflector (constructor-based context passing)
    template <typename T>
    requires ContextfulUnreflector<T>
    [[nodiscard]] T unreflect(const Reflected& data) const
    {
        if (const auto it = context.find(std::type_index(typeid(typename Unreflector<T>::ContextType))); it != context.end())
        {
            return Unreflector<T>{std::any_cast<typename Unreflector<T>::ContextType>(it->second)}(data, *this);
        }
        throw CannotDeserialize(
            "Deserialization context of type {} for type {} not found", NAMEOF_TYPE(typename Unreflector<T>::ContextType), NAMEOF_TYPE(T));
    }

    /// Overload for aggregate types without explicit unreflector (field-by-field deserialization)
    template <typename T>
    requires(!HasExplicitUnreflector<T>) && std::is_class_v<T> && std::is_aggregate_v<T>
    T unreflect(const Reflected& data) const
    {
        return unreflect_with_context_impl<T>(data);
    }

private:
    template <typename T>
    T unreflect_with_context_impl(const Reflected& data) const
    {
        const rfl::Generic& generic = *data;

        /// Handle different variant types in rfl::Generic and try to infer how to convert them to T
        return std::visit(
            [this](auto&& value) -> T
            {
                using ValueType = std::decay_t<decltype(value)>;

                /// Object - use field-by-field deserialization
                if constexpr (std::is_same_v<ValueType, rfl::Generic::Object>)
                {
                    return unreflect_object<T>(value);
                }
                /// Array - deserialize each element and construct target array type
                else if constexpr (std::is_same_v<ValueType, rfl::Generic::Array>)
                {
                    return unreflect_array<T>(value);
                }
                /// Primitives (bool, int64_t, double, string) - try direct construction or conversion
                else if constexpr (
                    std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int64_t> || std::is_same_v<ValueType, double>
                    || std::is_same_v<ValueType, std::string>)
                {
                    return unreflect_primitive<T>(value);
                }
                /// null - handle optional types
                else if constexpr (std::is_same_v<ValueType, std::nullopt_t>)
                {
                    return unreflect_null<T>();
                }
                else
                {
                    throw CannotDeserialize("Unexpected variant type for {}", NAMEOF_TYPE(T));
                }
            },
            generic.variant());
    }

    template <typename T>
    T unreflect_object(const rfl::Generic::Object& object) const
    {
        /// Build a tuple of deserialized values using compile-time iteration
        using NamedTupleType = rfl::named_tuple_t<T>;
        constexpr size_t numFields = NamedTupleType::size();

        auto valuesTuple = [&]<size_t... Is>(std::index_sequence<Is...>)
        { return std::make_tuple(unreflect_field<T, Is>(object)...); }(std::make_index_sequence<numFields>{});

        return construct_from_tuple<T>(std::move(valuesTuple));
    }

    template <typename T>
    T unreflect_array(const rfl::Generic::Array& array) const
    {
        if constexpr (requires {
                          typename T::value_type;
                          T().push_back(std::declval<typename T::value_type>());
                      })
        {
            /// T is a container (vector, list, etc.)
            T result;
            for (const auto& element : array)
            {
                result.push_back(this->unreflect<typename T::value_type>(Reflected{element}));
            }
            return result;
        }
        else
        {
            throw CannotDeserialize("Cannot deserialize array to type {}", NAMEOF_TYPE(T));
        }
    }

    template <typename T, typename PrimitiveType>
    T unreflect_primitive(const PrimitiveType& value) const
    {
        /// Only handle actual primitive-to-primitive conversions
        /// For complex types, this should not be called - they need field-by-field deserialization
        if constexpr (std::is_same_v<T, PrimitiveType>)
        {
            return value;
        }
        else if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, std::string> || std::is_enum_v<T>)
        {
            /// Target is primitive - try conversion
            if constexpr (std::is_constructible_v<T, PrimitiveType>)
            {
                return T{value};
            }
            else if constexpr (std::is_convertible_v<PrimitiveType, T>)
            {
                return static_cast<T>(value);
            }
            else
            {
                throw CannotDeserialize("Cannot convert primitive type to {}", NAMEOF_TYPE(T));
            }
        }
        else
        {
            /// Target is a complex type - this shouldn't be called
            throw CannotDeserialize("Cannot deserialize primitive to complex type {}", NAMEOF_TYPE(T));
        }
    }

    template <typename T>
    T unreflect_null() const
    {
        if constexpr (requires {
                          T{};
                          std::is_default_constructible_v<T>;
                      })
        {
            return T{};
        }
        else
        {
            throw CannotDeserialize("Cannot deserialize null to non-default-constructible type {}", NAMEOF_TYPE(T));
        }
    }

    template <typename T, size_t Index>
    auto unreflect_field(const rfl::Generic::Object& object) const
    {
        using NamedTupleType = rfl::named_tuple_t<T>;
        using FieldType = rfl::tuple_element_t<Index, typename NamedTupleType::Fields>;
        using FieldValueType = typename FieldType::Type;
        const auto fieldName = typename FieldType::Name().str();

        auto fieldResult = object.get(fieldName);
        if (!fieldResult.has_value())
        {
            throw CannotDeserialize("Missing field '{}' for type {}", fieldName, NAMEOF_TYPE(T));
        }

        return this->unreflect<FieldValueType>(Reflected{fieldResult.value()});
    }

    template <typename T, typename Tuple, size_t... Is>
    T construct_from_tuple_impl(Tuple&& values, std::index_sequence<Is...>) const
    {
        /// Construct T directly from the tuple of deserialized values
        /// This avoids calling rfl::from_generic which would bypass our context chain
        if constexpr (std::is_aggregate_v<T>)
        {
            /// For aggregates, use aggregate initialization
            return T{std::get<Is>(std::forward<Tuple>(values))...};
        }
        else
        {
            /// For non-aggregates, try make_from_tuple
            return std::make_from_tuple<T>(std::forward<Tuple>(values));
        }
    }

    template <typename T, typename Tuple>
    T construct_from_tuple(Tuple&& values) const
    {
        using NamedTupleType = rfl::named_tuple_t<T>;
        constexpr size_t numFields = NamedTupleType::size();
        return construct_from_tuple_impl<T>(std::forward<Tuple>(values), std::make_index_sequence<numFields>{});
    }
};

template <typename T>
[[nodiscard]] Reflected reflect(const T& data);

template <typename T>
struct Reflector;

namespace detail
{
template <typename T>
Reflected reflect_aggregate_impl(const T& data);
}

/// Overload for types with explicit Reflector specialization
/// Checks if Reflector<T> can be default-constructed (i.e., has a specialization)
/// This takes precedence over all other overloads
template <typename T>
requires requires { Reflector<T>{}; }
Reflected reflect(const T& data)
{
    return Reflector<T>{}(data);
}

/// Overload for primitive types (arithmetic, string, enum) without custom Reflector
template <typename T>
requires(std::is_arithmetic_v<T> || std::is_same_v<T, std::string> || std::is_enum_v<T>) && (!requires { Reflector<T>{}; })
Reflected reflect(const T& data)
{
    if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, std::string>)
    {
        return Reflected{rfl::Generic{data}};
    }
    else if constexpr (std::is_integral_v<T>)
    {
        return Reflected{rfl::Generic{static_cast<int64_t>(data)}};
    }
    else if constexpr (std::is_floating_point_v<T>)
    {
        return Reflected{rfl::Generic{static_cast<double>(data)}};
    }
    else if constexpr (std::is_enum_v<T>)
    {
        /// Serialize enums as their underlying integer value
        return Reflected{rfl::Generic{static_cast<int64_t>(static_cast<std::underlying_type_t<T>>(data))}};
    }
    else
    {
        static_assert(always_false_v<T>, "Unsupported primitive type");
    }
}

/// Overload for pointer types without custom Reflector
template <typename T>
requires std::is_pointer_v<T> && (!requires { Reflector<T>{}; })
Reflected reflect(const T& data)
{
    if (data == nullptr)
    {
        return Reflected{std::nullopt};
    }
    /// Dereference and reflect the pointed-to value
    return reflect(*data);
}

/// Overload for aggregate types without custom Reflector - use field-by-field reflection
template <typename T>
requires std::is_class_v<T> && std::is_aggregate_v<T> && (!requires { Reflector<T>{}; })
Reflected reflect(const T& data)
{
    return detail::reflect_aggregate_impl(data);
}

/// Fallback overload for types that don't match any other overload
/// This provides a clear compile-time error message
template <typename T>
Reflected reflect(const T&)
{
    static_assert(
        always_false_v<T>,
        "No reflection overload available for this type. "
        "Options: 1) Add a Reflector<T> specialization, "
        "2) Make the type an aggregate, "
        "3) For primitives/enums, ensure proper includes.");
}

namespace detail
{
/// Helper to reflect a single field at compile-time
/// Implementation in Reflection.hpp to ensure all reflect() overloads are visible
template <typename T, size_t Index>
auto reflect_field_at_index(const T& data);

/// Reflect an aggregate type field-by-field to build a Generic::Object
/// Implementation in Reflection.hpp to ensure all reflect() overloads are visible

}

/// Core Unreflector specializations

template <>
struct Reflector<Reflected>
{
    Reflected operator()(const Reflected& field) const { return field; }
};

template <>
struct Unreflector<Reflected>
{
    Reflected operator()(const Reflected& field, const ReflectionContext&) const { return field; }
};

/// Unreflector for primitive types (arithmetic and string)
template <Primitive T>
struct Unreflector<T>
{
    T operator()(const Reflected& data, const ReflectionContext&) const
    {
        auto optional = rfl::from_generic<T>(data);
        if (!optional.has_value())
        {
            throw CannotDeserialize("Failed to unreflect given data, rfl error: {}", optional.error().what());
        }
        return optional.value();
    }
};

/// Unreflector for enum types
template <typename T>
requires std::is_enum_v<T>
struct Unreflector<T>
{
    T operator()(const Reflected& data, const ReflectionContext&) const
    {
        return std::visit(
            [](const auto& value) -> T
            {
                using ValueType = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<ValueType, int64_t>)
                {
                    return static_cast<T>(value);
                }
                else if constexpr (std::is_integral_v<ValueType>)
                {
                    return static_cast<T>(static_cast<int64_t>(value));
                }
                throw CannotDeserialize("Expected integer for enum, got different type {}", NAMEOF_TYPE(ValueType));
            },
            data->variant());
    }
};

}
