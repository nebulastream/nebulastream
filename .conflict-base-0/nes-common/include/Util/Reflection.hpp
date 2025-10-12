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
#include <optional>
#include <utility>

#include <rfl/Generic.hpp>
#include <rfl/Result.hpp>
#include <rfl/from_generic.hpp>
#include <rfl/internal/has_reflector.hpp>
#include <rfl/to_generic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Reflection enables the serialization of arbitrary objects (E.g. LogicalOperators, Logical Functions, Traits, ...) and their
/// child objects into generic formats, such as JSON, without having to define all possible data types to serialize in a central
/// location, such as the protobuf files. This allows that new logical operators or functions can be created and their serialization
/// logic can be fully contained in their own classes, even if attributes of new data formats are being serialized.
///
/// For all reflectable classes must a Reflector struct be created to support the reflection of class instances into generic
/// Reflected objects and an Unreflector struct to support the creation of a class instance based on a given Reflected object.
///
/// Reflected objects contain data structures where each child data type is itself reflectable, e.g. has a Reflector and Unreflector
/// implementation. Most standard datatypes, such as bool, int, double, std::string, std::map, std::vector, std::nullopt_t, ... are
/// reflectable by default.
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
[[nodiscard]] Reflected reflect(const T& data);

template <typename T>
[[nodiscard]] T unreflect(const Reflected& data);

template <typename T>
struct Reflector;

template <typename T>
requires requires(T data) {
    { rfl::to_generic(data) } -> std::same_as<rfl::Generic>;
} && (!requires(T data) {
             { Reflector<T>{}(data) } -> std::same_as<Reflected>;
         })
Reflected reflect(const T& data)
{
    return Reflected{rfl::to_generic(data)};
}

template <typename T>
requires requires(T data) {
    { Reflector<T>{}(data) } -> std::same_as<Reflected>;
}
Reflected reflect(const T& data)
{
    return Reflector<T>{}(data);
}


template <typename T>
struct Unreflector;

template <typename T>
requires requires(rfl::Generic data) {
    { rfl::from_generic<T>(data) } -> std::same_as<rfl::Result<T>>;
} && (!requires(T data) {
             { Unreflector<T>{}(data) } -> std::same_as<T>;
         })
T unreflect(const Reflected& data)
{
    auto optional = rfl::from_generic<T>(data);
    if (!optional.has_value())
    {
        throw CannotDeserialize("Failed to unreflect given data");
    }
    return optional.value();
}

template <>
struct Reflector<Reflected>
{
    Reflected operator()(const Reflected& field) const { return field; }
};

template <>
struct Unreflector<Reflected>
{
    Reflected operator()(const Reflected& field) const { return field; }
};

}

namespace rfl
{

template <typename T>
requires requires(T data, NES::Reflected reflected) {
    { NES::Reflector<T>{}(data) } -> std::same_as<NES::Reflected>;
    { NES::Unreflector<T>{}(reflected) } -> std::same_as<T>;
}
struct Reflector<T>
{
    using ReflType = rfl::Generic;

    static rfl::Generic from(const T& value) { return NES::Reflector<T>{}(value); }

    static T to(const rfl::Generic& generic) { return NES::Unreflector<T>{}(generic); }
};

/// Partial specialization in case one of the directions is trivial
template <typename T>
requires requires(T data) {
    { NES::Reflector<T>{}(data) } -> std::same_as<NES::Reflected>;
} && (!requires(NES::Reflected reflected) { NES::Unreflector<T>{}(reflected); })
struct Reflector<T>
{
    using ReflType = rfl::Generic;

    static rfl::Generic from(const T& value) { return NES::Reflector<T>{}(value); }
};

template <typename T>
requires requires(NES::Reflected reflected) {
    { NES::Unreflector<T>{}(reflected) } -> std::same_as<T>;
} && (!requires(T data) { NES::Reflector<T>{}(data); })
struct Reflector<T>
{
    using ReflType = rfl::Generic;

    static T to(const rfl::Generic& generic) { return NES::Unreflector<T>{}(generic); }
};
}
