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
#include <Util/Reflection/ReflectionCore.hpp>

namespace NES
{

/// Opt-in interface for plugin authors to expose a serialization shape without writing
/// `Reflector<T>` / `Unreflector<T>` template specializations or a `detail::ReflectedT`
/// shadow struct. A type T satisfying `Reflectable` declares:
///
/// - a nested `T::Wire` aggregate naming the fields that go on the wire,
/// - a `wire() const -> T::Wire` member returning a wire snapshot of `*this`,
/// - a static `T::fromWire(T::Wire, const ReflectionContext&) -> T` factory.
///
/// The generic `Reflector<T>` and `Unreflector<T>` below pick up any such type
/// automatically. Authors who need to control reflection more tightly can still
/// provide explicit `Reflector<T>` / `Unreflector<T>` specializations — those win
/// over the generic via partial-ordering / explicit specialization rules.
template <typename T>
concept Reflectable = requires(const T& t, const ReflectionContext& ctx) {
    typename T::Wire;
    { t.wire() } -> std::same_as<typename T::Wire>;
    { T::fromWire(std::declval<typename T::Wire>(), ctx) } -> std::same_as<T>;
};

template <Reflectable T>
struct Reflector<T>
{
    Reflected operator()(const T& value) const { return reflect(value.wire()); }
};

template <Reflectable T>
struct Unreflector<T>
{
    T operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return T::fromWire(context.unreflect<typename T::Wire>(data), context);
    }
};

}
