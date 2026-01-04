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
#include <utility>

#include <rfl/Generic.hpp>
#include <rfl/Result.hpp>
#include <rfl/from_generic.hpp>
#include <rfl/to_generic.hpp>

namespace NES
{

struct Reflected
{
    ///NOLINTNEXTLINE
    Reflected(rfl::Generic value) : value(std::move(value)) { }

    ///NOLINTNEXTLINE
    operator rfl::Generic() const { return value; }

    const rfl::Generic& operator*() const { return value; }

    const rfl::Generic* operator->() const { return &value; }

private:
    rfl::Generic value;
};

template <typename T>
Reflected reflect(const T& data);

template <typename T>
T unreflect(const Reflected& data);

template <typename T>
requires requires(T data) {
    { rfl::to_generic(data) } -> std::same_as<rfl::Generic>;
}
Reflected reflect(const T& data)
{
    return Reflected{rfl::to_generic(data)};
}

template <typename T>
struct Reflector;

template <typename T>
requires requires(T data) {
    { Reflector<T>{}(data) } -> std::same_as<Reflected>;
}
Reflected reflect(const T& data)
{
    return Reflector<T>{}(data);
}

template <typename T, typename ReflType>
requires requires(T data, ReflType rfl) {
    { Reflector<T>{}(data) } -> std::same_as<ReflType>;
    { rfl::to_generic(rfl) } -> std::same_as<rfl::Generic>;
}
Reflected reflect(const T& data)
{
    return Reflected{rfl::to_generic(Reflector<T>{}(data))};
}

template <typename T>
requires requires(T data) {
    { rfl::from_generic<T>(data) } -> std::same_as<rfl::Result<T>>;
}
T unreflect(const Reflected& data)
{
    ///TODO add exception
    return rfl::from_generic<T>(data).value();
}

template <typename T>
struct Unreflector;

template <typename T>
requires requires(T data) {
    { Unreflector<T>{}(data) } -> std::same_as<T>;
}
T unreflect(const Reflected& data)
{
    return Unreflector<T>{}(data);
}

static auto test = rfl::to_generic<>("hi");

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
} // namespace rfl
